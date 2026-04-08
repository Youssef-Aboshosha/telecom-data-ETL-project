import yaml
import os
import sys
import re
import time
import logging
import argparse

from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta.tables import DeltaTable

from include.transformations.spark_utils import (
    get_spark_session,
    BRONZE_PATH,
    SILVER_PATH
)

SCHEMA_PATH = "/usr/local/airflow/include/configs/schemas.yaml"
METRICS_PATH = "/usr/local/airflow/metrics/silver_pipeline"


# ---------------------------------------------------------
# Logger Setup
# ---------------------------------------------------------

def setup_logger():
    logger = logging.getLogger("silver_layer")

    if not logger.handlers:
        logger.setLevel(logging.INFO)

        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
        )

        ch = logging.StreamHandler()
        ch.setFormatter(formatter)

        logger.addHandler(ch)

    return logger


logger = setup_logger()


# ---------------------------------------------------------
# Load YAML
# ---------------------------------------------------------

def load_schema_config():
    with open(SCHEMA_PATH, "r") as f:
        return yaml.safe_load(f)


# ---------------------------------------------------------
# snake_case
# ---------------------------------------------------------

def to_snake_case(name):
    name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    name = re.sub('([a-z0-9])([A-Z])', r'\1_\2', name)
    return name.lower()


# ---------------------------------------------------------
# Phone Normalization
# ---------------------------------------------------------

def normalize_phone(col):
    return (
        F.when(col.rlike("^01"), F.concat(F.lit("+20"), col.substr(2, 20)))
        .when(col.rlike("^20"), F.concat(F.lit("+"), col))
        .when(col.rlike("^\+"), col)
        .otherwise(F.concat(F.lit("+"), col))
    )


# ---------------------------------------------------------
# Outlier Detection
# ---------------------------------------------------------

def remove_outliers_iqr(df, column):
    quantiles = df.approxQuantile(column, [0.25, 0.75], 0.05)

    if len(quantiles) < 2:
        return df

    q1, q3 = quantiles
    iqr = q3 - q1

    lower = q1 - 1.5 * iqr
    upper = q3 + 1.5 * iqr

    return df.filter((F.col(column) >= lower) & (F.col(column) <= upper))


# ---------------------------------------------------------
# Latest Batch
# ---------------------------------------------------------

def get_latest_batch(df):
    latest_batch = df.select(F.max("_batch_id")).collect()[0][0]
    return df.filter(F.col("_batch_id") == latest_batch)


# ---------------------------------------------------------
# Date Validation
# ---------------------------------------------------------

def validate_dates(df):
    for c in df.schema.fields:
        if isinstance(c.dataType, TimestampType):
            df = df.filter(
                (F.col(c.name) > F.lit("2000-01-01")) &
                (F.col(c.name) < F.current_timestamp())
            )
    return df


# ---------------------------------------------------------
# Metrics Collector
# ---------------------------------------------------------

def collect_metrics(table_name, start_time, before_count, after_count):
    duration = time.time() - start_time

    drop_rate = 0
    if before_count > 0:
        drop_rate = (before_count - after_count) / before_count

    return {
        "table_name": table_name,
        "before_count": before_count,
        "after_count": after_count,
        "drop_rate": drop_rate,
        "duration_sec": duration,
        "processed_at": time.strftime("%Y-%m-%d %H:%M:%S")
    }


def write_metrics(spark, metrics):
    metrics_df = spark.createDataFrame([metrics])

    metrics_df.write.format("delta") \
        .mode("append") \
        .save(METRICS_PATH)


# ---------------------------------------------------------
# Transform Table
# ---------------------------------------------------------

def transform_table(spark, table_name, table_cfg):

    start_time = time.time()
    logger.info(f"🚀 Processing {table_name}")

    bronze_path = f"{BRONZE_PATH}/{table_name}"
    silver_path = f"{SILVER_PATH}/{table_name}"

    logger.info(f"Reading Bronze: {bronze_path}")

    df = spark.read.parquet(bronze_path)

    before_count = df.count()
    logger.info(f"Initial count: {before_count}")

    df = get_latest_batch(df)

    df = df.toDF(*[to_snake_case(c) for c in df.columns])

    select_exprs = []

    for col_name, meta in table_cfg["columns"].items():

        expr = F.col(col_name)
        dtype = meta["type"]

        if dtype == "timestamp" and meta.get("is_nano"):
            expr = (expr / 1000000000).cast(TimestampType())

        elif "decimal" in dtype:
            p, s = dtype.replace("decimal(", "").replace(")", "").split(",")
            expr = expr.cast(DecimalType(int(p), int(s)))

        else:
            expr = expr.cast(dtype)

        if col_name == "phone":
            expr = normalize_phone(expr)

        alias = meta.get("alias", col_name)
        select_exprs.append(expr.alias(alias))

    df = df.select(*select_exprs)

    pk = table_cfg.get("primary_key")
    final_pk = None

    if pk:
        final_pk = table_cfg["columns"][pk].get("alias", pk)
        df = df.filter(F.col(final_pk).isNotNull())

    # Null Handling
    string_cols = [c for c, t in df.dtypes if t == "string" and c != final_pk]
    numeric_cols = [c for c, t in df.dtypes if t in ["int", "bigint", "double", "decimal"] and c != final_pk]

    df = df.fillna("UNKNOWN", subset=string_cols)
    df = df.fillna(0, subset=numeric_cols)

    df = validate_dates(df)

    if pk:
        df = df.dropDuplicates([final_pk])

    metrics_cols = table_cfg.get("metrics", [])

    for metric in metrics_cols:
        if metric in df.columns:
            df = remove_outliers_iqr(df, metric)

    # Metadata
    df = (
        df
        .withColumn("silver_processed_at", F.current_timestamp())
        .withColumn("pipeline_run_id", F.lit(spark.sparkContext.applicationId))
        .withColumn("source_system", F.lit("telecom_source"))
    )

    after_count = df.count()
    logger.info(f"Final count: {after_count}")

    # Monitoring
    metrics = collect_metrics(table_name, start_time, before_count, after_count)

    if metrics["drop_rate"] > 0.3:
        logger.warning(f"⚠️ High drop rate: {metrics['drop_rate']}")

    write_metrics(spark, metrics)

    # Delta Write
    if not DeltaTable.isDeltaTable(spark, silver_path):

        df.write.format("delta").mode("overwrite").save(silver_path)
        logger.info(f"✅ Created {table_name}")
        return

    delta_table = DeltaTable.forPath(spark, silver_path)

    if pk:
        merge_condition = f"t.{final_pk} = s.{final_pk}"

        (
            delta_table.alias("t")
            .merge(df.alias("s"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    logger.info(f"✅ Upserted {table_name}")


# ---------------------------------------------------------
# Run Silver Layer
# ---------------------------------------------------------

def run_silver_layer():

    logger.info("🔥 Starting Silver Layer Pipeline")

    spark = get_spark_session("Silver_Layer")

    config = load_schema_config()

    for table_name, table_cfg in config["tables"].items():
        try:
            transform_table(spark, table_name, table_cfg)
        except Exception as e:
            logger.error(f"❌ Failed {table_name}: {str(e)}", exc_info=True)

    spark.stop()


    logger.info("✅ Silver Layer Completed")


##
# ---------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Silver Layer Transformations")
    parser.add_argument("--table", help="Specific table to process")
    args = parser.parse_args()

    if args.table:
        logger.info(f"🎯 Targeted Run: {args.table}")
        # استخدام try-finally لضمان إغلاق الـ Spark Session حتى لو حصل فشل
        spark_session = get_spark_session(f"Silver_Transform_{args.table}")
        try:
            full_config = load_schema_config()
            if args.table in full_config["tables"]:
                transform_table(spark_session, args.table, full_config["tables"][args.table])
            else:
                logger.error(f"❌ Table '{args.table}' not found in schemas.yaml")
                sys.exit(1)
        except Exception as e:
            logger.error(f"❌ Failed to process {args.table}: {str(e)}", exc_info=True)
            sys.exit(1)
        finally:
            spark_session.stop()
    else:
        run_silver_layer()