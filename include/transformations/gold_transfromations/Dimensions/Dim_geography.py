import sys
from pyspark.sql import functions as F

# Ensure project root is in path
sys.path.append('/usr/local/airflow')
from include.transformations.spark_utils import get_spark_session, SILVER_PATH, GOLD_PATH

def build_dim_geography():
    """
    Builds the Geography Dimension by joining cities and countries.
    Uses the 'Metadata-Last' approach to ensure schema consistency 
    and avoid Nullability errors during the creation of the Unknown row.
    """
    print("🌍 Spark Session is starting for Geography...")
    spark = get_spark_session("gold_dim_geography")
    
    # ------------------------------------------------
    # 1. Load Silver Tables
    # ------------------------------------------------
    print("📊 Loading Silver tables...")
    cities = spark.read.format("delta").load(f"{SILVER_PATH}/cities")
    countries = spark.read.format("delta").load(f"{SILVER_PATH}/countries")

    # ------------------------------------------------
    # 2. Denormalize Geography Hierarchy
    # ------------------------------------------------
    print("🔗 Joining Cities and Countries...")
    enriched = cities.alias("ci").join(
        countries.alias("co"),
        "country_id",
        "left"
    )

    # ------------------------------------------------
    # 3. Base Standardized Selection
    # ------------------------------------------------
    # We select base columns first without Metadata (SK/Timestamp)
    # to ensure a clean union with the Unknown row.
    dim_geography_base = enriched.select(
        F.col("ci.city_id"),
        F.col("ci.city_name"),
        F.col("co.country_id"),
        F.col("co.country_name"),
        F.col("co.country_code")
    ).fillna({
        "city_name": "UNKNOWN_CITY",
        "country_name": "UNKNOWN_COUNTRY",
        "country_code": "NA"
    })

    # ------------------------------------------------
    # 4. Create Unknown Row (Base Columns Only)
    # ------------------------------------------------
    # Placeholder for late-arriving data or missing references
    unknown_data = [{
        "city_id": -1,
        "city_name": "UNKNOWN",
        "country_id": -1,
        "country_name": "UNKNOWN",
        "country_code": "NA"
    }]
    
    # Create DF using the same base schema
    unknown_base_df = spark.createDataFrame(unknown_data, schema=dim_geography_base.schema)
    
    # Combine actual data with the unknown row
    final_base_df = dim_geography_base.unionByName(unknown_base_df)

    # ------------------------------------------------
    # 5. Add Metadata (SK & Timestamp) to Everything
    # ------------------------------------------------
    # Applying these transformations to the entire combined set 
    # guarantees 100% schema consistency.
    final_dim_geography = final_base_df.withColumn(
        "geography_sk", F.expr("uuid()")
    ).withColumn(
        "gold_processed_at", F.current_timestamp()
    )

    # ------------------------------------------------
    # 6. Write to Gold Layer
    # ------------------------------------------------
    target_path = f"{GOLD_PATH}/dim_geography"
    print(f"💾 Writing to Gold: {target_path}")
    
    final_dim_geography.write.format("delta") \
        .mode("overwrite") \
        .save(target_path)
    
    print(f"✅ DONE! Geography Dimension ready with {final_dim_geography.count()} rows.")
    spark.stop()

# تأكد إن السطرين دول في آخر الملف تماماً ومحاذاتهم لليسار 100%
if __name__ == "__main__":
    build_dim_geography()