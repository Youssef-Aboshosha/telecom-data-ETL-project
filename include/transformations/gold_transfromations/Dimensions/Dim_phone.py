import sys
from pyspark.sql import functions as F

# إعداد المسارات
sys.path.append('/usr/local/airflow')
from include.transformations.spark_utils import get_spark_session, SILVER_PATH, GOLD_PATH

def build_dim_phone():
    print("🚀 Spark Session is starting...")
    spark = get_spark_session("gold_dim_phone")
    
    print("📊 Loading Silver tables...")
    phones = spark.read.format("delta").load(f"{SILVER_PATH}/phone_numbers")
    sims = spark.read.format("delta").load(f"{SILVER_PATH}/sim_cards")
    subs = spark.read.format("delta").load(f"{SILVER_PATH}/subscriptions")
    plans = spark.read.format("delta").load(f"{SILVER_PATH}/service_plans")

    print("🔗 Joining data...")
    # تحضير الجداول لتجنب الأسماء المتكررة
    sims_prep = sims.withColumnRenamed("status", "sim_status")
    subs_prep = subs.withColumnRenamed("status", "subscription_status")

    enriched = phones.alias("p") \
        .join(sims_prep.alias("s"), "sim_id", "left") \
        .join(subs_prep.alias("sub"), "phone_id", "left") \
        .join(plans.alias("pl"), "plan_id", "left")

    print("✨ Selecting final columns...")
    dim_phone = enriched.select(
        F.expr("uuid()").alias("phone_sk"),
        F.col("p.phone_id"),
        F.col("p.phone_number"),
        F.col("s.sim_number"),
        F.col("s.sim_status"),
        F.col("sub.plan_id"),
        F.col("pl.plan_name"),
        F.col("sub.subscription_status"),
        F.current_timestamp().alias("gold_processed_at")
    ).dropDuplicates(["phone_id"])

    print(f"💾 Writing to Gold: {GOLD_PATH}/dim_phone")
    dim_phone.write.format("delta").mode("overwrite").save(f"{GOLD_PATH}/dim_phone")
    
    print("✅ DONE!")
    spark.stop()

# نداء الفانكشن مباشرة بدون أي شروط
print("🎬 Script Triggered")
build_dim_phone()