import sys
import os
sys.path.append('/usr/local/airflow')
from include.transformations.spark_utils import get_spark_session, GOLD_PATH

def inspect_all_gold_tables():
    spark = get_spark_session("inspect_gold")
    spark.sparkContext.setLogLevel("ERROR")
    
    tables = [
        "dim_customer",
        "dim_date",
        "dim_geography",
        "dim_phone",
        "dim_plan_catalog",
        "fact_usage"
    ]
    
    print("\n" + "="*60)
    print("🔍 LIVE GOLD LAYER SCHEMA INSPECTION")
    print("="*60)
    
    for table in tables:
        path = f"{GOLD_PATH}/{table}"
        print(f"\n📂 Table: {table.upper()}")
        print("-" * 30)
        
        try:
            df = spark.read.format("delta").load(path)
            print(f"{'Column Name':<25} | {'Data Type':<15}")
            print("-" * 45)
            for field in df.schema:
                print(f"{field.name:<25} | {field.dataType.simpleString():<15}")
        except Exception:
            print(f"⚠️ Error: Could not load {table}. Path might not exist yet.")
            
    print("\n" + "="*60)
    spark.stop()

if __name__ == "__main__":
    inspect_all_gold_tables()
