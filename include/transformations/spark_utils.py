from pyspark.sql import SparkSession
import os

def get_spark_session(app_name="TelecomDataPlatform"):
    """
    Creates or retrieves a Spark Session optimized for local 
    container execution and Snowflake connectivity.
    """
    spark = (SparkSession.builder
        .appName(app_name)
        # Performance: Maximize local cores
        .config("spark.executor.cores", "2")
        .config("spark.executor.memory", "2g")
        # Optimization: Parquet vectorization for faster reads
        .config("spark.sql.parquet.enableVectorizedReader", "true")
        # Essential for Snowflake/S3 integration later
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate())
    
    # Setting log level to WARN to avoid flooding Airflow logs with INFO
    spark.sparkContext.setLogLevel("WARN")
    
    return spark