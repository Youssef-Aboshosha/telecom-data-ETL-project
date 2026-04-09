import os
from pyspark.sql import functions as F
from include.transformations.spark_utils import get_spark_session

# Define standard paths
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
SILVER_PATH = os.path.join(BASE_DIR, "include", "data", "silver")
GOLD_PATH = os.path.join(BASE_DIR, "include", "data", "gold")

def run_gold_pipeline():
    print("🚀 Engine Started: Gold Layer (Galaxy Schema)")
    spark = get_spark_session("Gold_Execution")

    # Ensure Gold output directory exists
    os.makedirs(GOLD_PATH, exist_ok=True)

    # =========================================================================
    # 1. SHARED DIMENSION: Dim_Customer
    # =========================================================================
    print("🛠️ Building Shared Dimension: Dim_Customer...")
    customers_df = spark.read.parquet(os.path.join(SILVER_PATH, "customers"))
    addresses_df = spark.read.parquet(os.path.join(SILVER_PATH, "addresses"))

    # Join customers and addresses, create a deterministic Surrogate Key using MD5
    dim_customer = customers_df.join(addresses_df, on="customer_id", how="left") \
        .withColumn("dim_customer_sk", F.md5(F.col("customer_id").cast("string"))) \
        .withColumn("_inserted_at", F.current_timestamp())

    dim_customer.write.mode("overwrite").parquet(os.path.join(GOLD_PATH, "dim_customer"))
    print("✅ Dim_Customer saved successfully.")

    # =========================================================================
    # 2. FACT TABLE: Fact_Usage (Calls, SMS, Data)
    # =========================================================================
    print("🛠️ Building Fact: Fact_Usage...")
    calls_df = spark.read.parquet(os.path.join(SILVER_PATH, "call_records"))
    sms_df = spark.read.parquet(os.path.join(SILVER_PATH, "sms"))
    data_df = spark.read.parquet(os.path.join(SILVER_PATH, "data_sessions"))

    # Standardize schemas before union
    # Note: Adjust the 'F.col()' mappings to exactly match your silver layer column names
    calls_std = calls_df.select(
        F.col("customer_id"),
        F.col("call_time").alias("usage_timestamp"),
        F.lit("CALL").alias("usage_type"),
        F.lit("MINUTES").alias("unit_metric"),
        F.col("duration").alias("units"),
        F.col("cost").alias("cost")
    )

    sms_std = sms_df.select(
        F.col("customer_id"),
        F.col("sms_time").alias("usage_timestamp"),
        F.lit("SMS").alias("usage_type"),
        F.lit("COUNT").alias("unit_metric"),
        F.lit(1).alias("units"), # SMS is usually billed per message
        F.col("cost").alias("cost")
    )

    data_std = data_df.select(
        F.col("customer_id"),
        F.col("session_time").alias("usage_timestamp"),
        F.lit("DATA").alias("usage_type"),
        F.lit("MB").alias("unit_metric"),
        F.col("data_mb").alias("units"),
        F.col("cost").alias("cost")
    )

    # Union the usage sources
    fact_usage = calls_std.unionByName(sms_std).unionByName(data_std)

    # Add Keys (Primary UUID, Foreign Key to Dim_Customer) and partitioning columns
    fact_usage = fact_usage \
        .withColumn("fact_usage_sk", F.expr("uuid()")) \
        .withColumn("dim_customer_sk", F.md5(F.col("customer_id").cast("string"))) \
        .withColumn("usage_date", F.to_date(F.col("usage_timestamp"))) \
        .withColumn("_inserted_at", F.current_timestamp())

    # Write Fact_Usage partitioned by date for optimized downstream querying
    fact_usage.write.mode("overwrite") \
        .partitionBy("usage_date", "usage_type") \
        .parquet(os.path.join(GOLD_PATH, "fact_usage"))
    print("✅ Fact_Usage saved successfully.")

    # =========================================================================
    # 3. FACT TABLE: Fact_Financials (Invoices, Payments)
    # =========================================================================
    print("🛠️ Building Fact: Fact_Financials...")
    invoices_df = spark.read.parquet(os.path.join(SILVER_PATH, "invoices"))
    payments_df = spark.read.parquet(os.path.join(SILVER_PATH, "payments"))

    invoices_std = invoices_df.select(
        F.col("invoice_id").alias("transaction_ref"),
        F.col("customer_id"),
        F.col("invoice_date").alias("transaction_timestamp"),
        F.lit("INVOICE").alias("transaction_type"),
        F.col("amount").alias("amount")
    )

    payments_std = payments_df.select(
        F.col("payment_id").alias("transaction_ref"),
        F.col("customer_id"),
        F.col("payment_date").alias("transaction_timestamp"),
        F.lit("PAYMENT").alias("transaction_type"),
        F.col("amount").alias("amount")
    )

    fact_financials = invoices_std.unionByName(payments_std) \
        .withColumn("fact_financial_sk", F.expr("uuid()")) \
        .withColumn("dim_customer_sk", F.md5(F.col("customer_id").cast("string"))) \
        .withColumn("transaction_date", F.to_date(F.col("transaction_timestamp"))) \
        .withColumn("_inserted_at", F.current_timestamp())

    fact_financials.write.mode("overwrite") \
        .partitionBy("transaction_date", "transaction_type") \
        .parquet(os.path.join(GOLD_PATH, "fact_financials"))
    print("✅ Fact_Financials saved successfully.")

    spark.stop()
    print("🛑 Engine Stopped")

if __name__ == "__main__":
    run_gold_pipeline()