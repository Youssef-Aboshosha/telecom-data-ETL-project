from include.transformations.spark_utils import get_spark_session, GOLD_PATH
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StructType, StructField, DateType, StringType, BooleanType
import datetime

def save_gold_table(df, table_name):
    """
    Modular write function. 
    Currently saves to Delta, but ready to be extended for Snowflake.
    """
    path = f"{GOLD_PATH}/{table_name}"
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(path)
    print(f"✅ Table {table_name} saved to {path}")

def build_dim_date():
    spark = get_spark_session("gold_dim_date")
    print("📅 Building Dim_Date...")

    # 1. Generate the date range
    start_date = datetime.date(2020, 1, 1)
    end_date = datetime.date(2035, 12, 31)
    total_days = (end_date - start_date).days

    date_range_df = spark.range(0, total_days + 1).select(
        F.expr(f"date_add('{start_date}', cast(id as int))").alias("date")
    )

    # 2. Transform into Dimension attributes
    dim_date = date_range_df.select(
        F.date_format("date", "yyyyMMdd").cast(IntegerType()).alias("date_key"),
        F.col("date"),
        F.year("date").alias("year"),
        F.quarter("date").alias("quarter"),
        F.month("date").alias("month"),
        F.date_format("date", "MMMM").alias("month_name"),
        F.weekofyear("date").alias("week_of_year"),
        F.dayofmonth("date").alias("day_of_month"),
        F.dayofweek("date").alias("day_of_week"),
        F.date_format("date", "EEEE").alias("day_name"),
        F.when(F.dayofweek("date").isin(1, 7), True).otherwise(False).alias("is_weekend")
    )

    # 3. Add the "Unknown" row (Late Arriving / Missing Data Handling)
    # We define the schema explicitly to match the dim_date DF
    unknown_row = [(
        -1,                      # date_key
        None,                    # date
        0,                       # year
        0,                       # quarter
        0,                       # month
        "UNKNOWN",               # month_name
        0,                       # week_of_year
        0,                       # day_of_month
        0,                       # day_of_week
        "UNKNOWN",               # day_name
        False                    # is_weekend
    )]
    
    unknown_df = spark.createDataFrame(unknown_row, schema=dim_date.schema)

    # 4. Union and Finalize
    final_dim_date = dim_date.union(unknown_df)

    # 5. Modular Write
    save_gold_table(final_dim_date, "dim_date")

    spark.stop()

if __name__ == "__main__":
    build_dim_date()