import os
import sys

# Ensure path is correct inside the Container
sys.path.append("/usr/local/airflow")

from include.transformations.spark_utils import get_spark_session, ARCHIVE_PATH

def print_file_card(index, total, file_name, size_mb, df):
    """
    Presentation Logic: Prints a structured card for each data asset.
    """
    # 1. Prepare Schema string with types
    schema_info = [f"{col}:{dtype}" for col, dtype in df.dtypes]
    
    # We split the schema into chunks of 4 columns per line for readability
    chunk_size = 4
    schema_lines = [", ".join(schema_info[i:i + chunk_size]) 
                    for i in range(0, len(schema_info), chunk_size)]
    
    # 2. Print the Card
    print(f"📁 [{index}/{total}] File: {file_name}")
    print(f"   ⚖️  Size: {size_mb} MB")
    print(f"   🔢 Columns Count: {len(df.columns)}")
    print(f"   👥 Rows Count: {df.count():,}") # Added thousands separator
    print(f"   📜 Full Schema (Name:Type):")
    for line in schema_lines:
        print(f"      {line}")
    print("-" * 60)

def run_professional_catalog():
    spark = get_spark_session("Executive_Data_Catalog")
    
    if not os.path.exists(ARCHIVE_PATH):
        print(f"❌ ARCHIVE_PATH not found: {ARCHIVE_PATH}")
        return

    files = sorted([f for f in os.listdir(ARCHIVE_PATH) if f.endswith(".parquet")])
    total_files = len(files)

    print("\n" + "="*25 + " TELECOM DATA CATALOG " + "="*25 + "\n")

    for i, file_name in enumerate(files, 1):
        try:
            file_path = os.path.join(ARCHIVE_PATH, file_name)
            size_mb = round(os.path.getsize(file_path) / (1024 * 1024), 3)
            
            # Read file
            df = spark.read.parquet(file_path)
            
            # Print the card
            print_file_card(i, total_files, file_name, size_mb, df)
            
        except Exception as e:
            print(f"❌ [{i}/{total_files}] Error in {file_name}:")
            print(f"   {str(e)[:150]}...")
            print("-" * 60)

    print("\n" + "="*20 + " CATALOG GENERATION COMPLETE " + "="*20 + "\n")
    spark.stop()

if __name__ == "__main__":
    run_professional_catalog()