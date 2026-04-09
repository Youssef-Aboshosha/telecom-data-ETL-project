import sys
from decimal import Decimal
from pyspark.sql import functions as F
from include.transformations.spark_utils import get_spark_session, SILVER_PATH, GOLD_PATH

def build_dim_plan_catalog():
    spark = get_spark_session("gold_dim_plan_catalog")
    print("📋 Building Unified Dim_Plan_Catalog...")

    # 1. Load Silver Tables
    plans = spark.read.format("delta").load(f"{SILVER_PATH}/service_plans")
    addons = spark.read.format("delta").load(f"{SILVER_PATH}/plan_addons")

    # 2. Standardize columns to a common base schema
    plans_final = plans.select(
        F.col("plan_id").alias("source_id"),
        F.col("plan_name").alias("item_name"),
        F.lit("BASE_PLAN").alias("item_type"),
        F.col("price").cast("decimal(10,2)")
    )

    addons_final = addons.select(
        F.col("addon_id").alias("source_id"),
        F.col("addon_name").alias("item_name"),
        F.lit("ADDON").alias("item_type"),
        F.col("price").cast("decimal(10,2)")
    )

    # 3. Union the base data first
    catalog_base = plans_final.unionByName(addons_final)

    # 4. Create the Unknown Row with the SAME base schema
    unknown_data = [{
        "source_id": -1,
        "item_name": "UNKNOWN ITEM",
        "item_type": "UNKNOWN",
        "price": Decimal("0.00")
    }]
    
    # هنا السر: نستخدم schema=catalog_base.schema (الأعمدة الـ 4 الأساسية فقط)
    unknown_base_df = spark.createDataFrame(unknown_data, schema=catalog_base.schema)

    # 5. Union the base data and the unknown row
    final_base_df = catalog_base.unionByName(unknown_base_df)

    # 6. NOW add Metadata to the entire combined dataset
    # هذا يضمن أن الـ SK والـ Timestamp يطبقون على الجميع بنفس القواعد
    final_df = final_base_df.withColumn(
        "plan_sk", 
        F.sha2(F.concat_ws("||", F.col("source_id"), F.col("item_type")), 256)
    ).withColumn("processed_at", F.current_timestamp())

    # 7. Save to Gold
    target_path = f"{GOLD_PATH}/dim_plan_catalog"
    final_df.write.format("delta").mode("overwrite").save(target_path)
    
    print(f"✅ Dim_Plan_Catalog built successfully with {final_df.count()} records.")
    spark.stop()

if __name__ == "__main__":
    build_dim_plan_catalog()