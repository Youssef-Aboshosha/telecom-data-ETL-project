from include.transformations.spark_utils import get_spark_session
from pyspark.sql import functions as F
import os

def ingest_landing_to_bronze():
    spark = get_spark_session("BronzeIngestion")
    
    # Define Paths (Using relative paths for Docker compatibility)
    landing_path = "data/landing/telcom_raw"
    bronze_path = "data/bronze/telecom_events"
    
    print(f"Reading files from {landing_path}...")
    
    # Read all parquet files in the folder
    raw_df = spark.read.parquet(landing_path)
    
    # Senior Move: Add ingestion metadata
    # This helps us track WHEN the data entered our platform
    bronze_df = raw_df.withColumn("_ingested_at", F.current_timestamp()) \
                      .withColumn("_source_file", F.input_file_name())
    
    # Write to Bronze (Overwrite for this portfolio, Append in real-time prod)
    print(f"Writing to {bronze_path}...")
    bronze_df.write.mode("overwrite").parquet(bronze_path)
    
    print("Bronze Layer Ingestion Complete.")
    spark.stop()

if __name__ == "__main__":
    ingest_landing_to_bronze()