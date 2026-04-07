from pyspark.sql import SparkSession
import os
import yaml
from delta import configure_spark_with_delta_pip

# Paths inside the container
BASE_DATA_PATH = "/usr/local/airflow/include/data" 
LANDING_PATH = os.path.join(BASE_DATA_PATH, "landing", "telcom_sanitized")
ARCHIVE_PATH = os.path.join(BASE_DATA_PATH, "landing", "archive")
BRONZE_PATH = os.path.join(BASE_DATA_PATH, "bronze")
SILVER_PATH = os.path.join(BASE_DATA_PATH, "silver")
GOLD_PATH = os.path.join(BASE_DATA_PATH, "gold")
CONFIG_PATH = "/usr/local/airflow/include/configs/pipeline_config.yaml"

def load_pipeline_config():
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, 'r') as f:
            return yaml.safe_load(f)
    return {"tables": {}}

def get_spark_session(app_name="Telecom_Data_Pipeline"):
    """
    Upgraded Spark Session to support Delta Lake 3.x
    """
    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0") \
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED") \
        .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED") \
        .config("spark.sql.legacy.parquet.nanosAsLong", "true") \
        .config("spark.sql.parquet.nanosAsLong", "true") \
        .config("spark.sql.parquet.enableVectorizedReader", "false") \
        .config("spark.sql.shuffle.partitions", "4") # Optimized for local Astro/Docker

    # This wrapper ensures all Delta-related Python dependencies are linked correctly
    return configure_spark_with_delta_pip(builder).getOrCreate()