import sys
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DecimalType

sys.path.append('/usr/local/airflow')

from include.transformations.spark_utils import (
    get_spark_session,
    SILVER_PATH,
    GOLD_PATH
)

def build_fact_usage():

    spark = get_spark_session("gold_fact_usage")

    print("🚀 Building Fact_Usage...")

    # ------------------------------------------------
    # Load Silver Tables
    # ------------------------------------------------

    calls = spark.read.format("delta").load(f"{SILVER_PATH}/call_records")
    sms = spark.read.format("delta").load(f"{SILVER_PATH}/sms_records")
    data = spark.read.format("delta").load(f"{SILVER_PATH}/data_sessions")

    subscriptions = spark.read.format("delta").load(f"{SILVER_PATH}/subscriptions")

    # ------------------------------------------------
    # Load Dimensions
    # ------------------------------------------------

    dim_customer = spark.read.format("delta").load(f"{GOLD_PATH}/dim_customer") \
        .filter("is_current = true") \
        .select("customer_id", "customer_sk")

    dim_phone = spark.read.format("delta").load(f"{GOLD_PATH}/dim_phone") \
        .select("phone_id", "phone_sk")

    # ------------------------------------------------
    # CALLS
    # ------------------------------------------------

    calls_mapped = calls.select(

        "phone_id",

        F.col("call_date").alias("event_timestamp"),

        F.lit("VOICE").alias("usage_type"),

        F.col("duration").cast("double").alias("quantity"),

        F.lit("SECONDS").alias("unit"),

        F.col("cost").cast(DecimalType(12,2)).alias("cost")

    )

    # ------------------------------------------------
    # SMS
    # ------------------------------------------------

    sms_mapped = sms.select(

        "phone_id",

        F.col("sent_date").alias("event_timestamp"),

        F.lit("SMS").alias("usage_type"),

        F.lit(1.0).alias("quantity"),

        F.lit("COUNT").alias("unit"),

        F.lit(0).cast(DecimalType(12,2)).alias("cost")

    )

    # ------------------------------------------------
    # DATA
    # ------------------------------------------------

    data_mapped = data.select(

        "phone_id",

        F.col("session_date").alias("event_timestamp"),

        F.lit("DATA").alias("usage_type"),

        F.col("data_used").cast("double").alias("quantity"),

        F.lit("MB").alias("unit"),

        F.lit(0).cast(DecimalType(12,2)).alias("cost")

    )

    # ------------------------------------------------
    # UNION
    # ------------------------------------------------

    usage_union = calls_mapped.unionByName(sms_mapped).unionByName(data_mapped)

    # ------------------------------------------------
    # Join to subscriptions → customer
    # ------------------------------------------------

    usage_with_customer = usage_union \
        .join(subscriptions.select("phone_id", "customer_id"), "phone_id", "left")

    # ------------------------------------------------
    # Join Dimensions
    # ------------------------------------------------

    final_fact = usage_with_customer \
        .join(dim_customer, "customer_id", "left") \
        .join(dim_phone, "phone_id", "left") \
        .withColumn("date_key", F.date_format("event_timestamp","yyyyMMdd").cast(IntegerType())) \
        .withColumn("year", F.year("event_timestamp")) \
        .withColumn("month", F.month("event_timestamp")) \
        .withColumn("fact_usage_sk", F.expr("uuid()")) \
        .withColumn("gold_processed_at", F.current_timestamp()) \
        .select(

            "fact_usage_sk",

            "date_key",

            "customer_sk",

            "phone_sk",

            "usage_type",

            "quantity",

            "unit",

            "cost",

            "year",

            "month",

            "gold_processed_at"

        )

    # ------------------------------------------------
    # Write Gold Table
    # ------------------------------------------------

    target_path = f"{GOLD_PATH}/fact_usage"

    final_fact.write \
        .format("delta") \
        .mode("append") \
        .partitionBy("year","month") \
        .save(target_path)

    print(f"✅ Fact_Usage created successfully")

    spark.stop()


if __name__ == "__main__":

    build_fact_usage()