import os
import sys
from pyspark.sql import functions as F

# إضافة المسار عشان الـ imports تشتغل
sys.path.append('/usr/local/airflow')
from include.transformations.spark_utils import get_spark_session, SILVER_PATH

def run_profiler():
    spark = get_spark_session("Silver_Data_Profiler")
    
    print(f"\n🚀 Checking path: {SILVER_PATH}")
    
    # 1. اكتشاف الجداول
    try:
        tables = [f for f in os.listdir(SILVER_PATH) if os.path.isdir(os.path.join(SILVER_PATH, f))]
        print(f"📊 Found {len(tables)} tables to profile.\n")
    except Exception as e:
        print(f"❌ Error listing directory: {e}")
        return

    # 2. البروفايلينج
    for table in tables:
        try:
            print(f"📄 Table: {table}")
            df = spark.read.format("delta").load(os.path.join(SILVER_PATH, table))
            
            row_count = df.count()
            print(f"🔢 Total Rows: {row_count}")
            
            print("🛠️ Column Types:")
            for col_name, dtype in df.dtypes:
                print(f"   - {col_name}: {dtype}")
            
            print("\n👀 Preview (Top 3):")
            df.show(3, truncate=False)
            print("="*80)
            
        except Exception as e:
            print(f"⚠️ Error profiling {table}: {str(e)}")

    spark.stop()

if __name__ == "__main__":
    run_profiler()