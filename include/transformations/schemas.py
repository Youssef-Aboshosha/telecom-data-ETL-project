from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, TimestampType, DecimalType, DoubleType

# --- [GROUP 1: MASTER DATA / DIMENSIONS] ---

CUSTOMERS_SILVER_SCHEMA = StructType([
    StructField("customer_id", LongType(), False),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("segment_id", IntegerType(), True),
    StructField("city_id", IntegerType(), True),
    StructField("status", StringType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("_processed_at", TimestampType(), True)
])

BRANCHES_SILVER_SCHEMA = StructType([
    StructField("branch_id", LongType(), False),
    StructField("branch_name", StringType(), True),
    StructField("city_id", LongType(), True),
    StructField("_processed_at", TimestampType(), True)
])

CITIES_SILVER_SCHEMA = StructType([
    StructField("city_id", LongType(), False),
    StructField("city_name", StringType(), True),
    StructField("country_id", LongType(), True),
    StructField("_processed_at", TimestampType(), True)
])

COUNTRIES_SILVER_SCHEMA = StructType([
    StructField("country_id", LongType(), False),
    StructField("country_name", StringType(), True),
    StructField("country_code", StringType(), True),
    StructField("_processed_at", TimestampType(), True)
])

SERVICE_PLANS_SILVER_SCHEMA = StructType([
    StructField("plan_id", LongType(), False),
    StructField("plan_name", StringType(), True),
    StructField("price", DecimalType(10, 2), True),
    StructField("_processed_at", TimestampType(), True)
])

SUBSCRIPTIONS_SILVER_SCHEMA = StructType([
    StructField("subscription_id", LongType(), False),
    StructField("customer_id", LongType(), True),
    StructField("phone_id", LongType(), True),
    StructField("plan_id", LongType(), True),
    StructField("status", StringType(), True),
    StructField("start_date", TimestampType(), True),
    StructField("_processed_at", TimestampType(), True)
])

# --- [GROUP 2: USAGE & NETWORK FACTS] ---

CALL_RECORDS_SILVER_SCHEMA = StructType([
    StructField("call_id", LongType(), False),
    StructField("phone_id", LongType(), True),
    StructField("duration", LongType(), True),
    StructField("cost", DecimalType(10, 2), True),
    StructField("call_date", TimestampType(), True),
    StructField("_processed_at", TimestampType(), True)
])

DATA_SESSIONS_SILVER_SCHEMA = StructType([
    StructField("session_id", LongType(), False),
    StructField("phone_id", LongType(), True),
    StructField("data_used", DecimalType(15, 4), True), # KB or MB
    StructField("session_date", TimestampType(), True),
    StructField("_processed_at", TimestampType(), True)
])

SMS_RECORDS_SILVER_SCHEMA = StructType([
    StructField("sms_id", LongType(), False),
    StructField("phone_id", LongType(), True),
    StructField("message_text", StringType(), True),
    StructField("sent_date", TimestampType(), True),
    StructField("_processed_at", TimestampType(), True)
])

# --- [GROUP 3: FINANCIAL FACTS] ---

INVOICES_SILVER_SCHEMA = StructType([
    StructField("invoice_id", LongType(), False),
    StructField("customer_id", LongType(), True),
    StructField("total_amount", DecimalType(12, 2), True),
    StructField("status", StringType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("_processed_at", TimestampType(), True)
])

PAYMENTS_SILVER_SCHEMA = StructType([
    StructField("payment_id", LongType(), False),
    StructField("invoice_id", LongType(), True),
    StructField("amount", DecimalType(12, 2), True),
    StructField("payment_date", TimestampType(), True),
    StructField("_processed_at", TimestampType(), True)
])

BALANCE_TRANSACTIONS_SILVER_SCHEMA = StructType([
    StructField("txn_id", LongType(), False),
    StructField("phone_id", LongType(), True),
    StructField("amount", DecimalType(12, 2), True),
    StructField("txn_date", TimestampType(), True),
    StructField("_processed_at", TimestampType(), True)
])

# --- [GROUP 4: ANALYTICS & PREDICTIONS] ---

CHURN_PREDICTIONS_SILVER_SCHEMA = StructType([
    StructField("prediction_id", LongType(), False),
    StructField("customer_id", LongType(), True),
    StructField("prediction_date", TimestampType(), True),
    StructField("churn_probability", DoubleType(), True),
    StructField("risk_level", StringType(), True),
    StructField("actual_churn", StringType(), True),
    StructField("_processed_at", TimestampType(), True)
])

# Add this at the end of schemas.py
print("Schemas loaded successfully!")
print(f"Customer Schema fields: {len(CUSTOMERS_SILVER_SCHEMA.fields)}")