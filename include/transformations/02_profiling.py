import os
import sys
from pyspark.sql import functions as F

# إضافة المسار لضمان عمل الـ Imports داخل الحاوية
sys.path.append("/usr/local/airflow")

from include.transformations.spark_utils import get_spark_session, BRONZE_PATH

def profile_table(spark, table_name):
    """
    يعرض اسم الملف، عدد الصفوف، أول 3 سجلات، وأنواع البيانات فقط.
    """
    file_path = f"{BRONZE_PATH}/{table_name}"
    
    # تحميل البيانات
    df = spark.read.parquet(file_path)
    total_rows = df.count()
    
    # 1. عرض اسم الجدول وعدد الصفوف
    print("=" * 80)
    print(f"📄 Table: {table_name}")
    print(f"🔢 Total Rows: {total_rows:,}")
    print("=" * 80)

    if total_rows == 0:
        print(f"⚠️ Table {table_name} is empty.")
        return

    # 2. عرض أول 3 سجلات
    print("\n👀 Preview (First 3 records):")
    df.show(3, truncate=False)

    # 3. عرض أنواع البيانات لكل عمود
    print("\n🛠️ Column Data Types:")
    print(f"{'Column Name':<40} | {'Data Type':<20}")
    print("-" * 65)
    for col_name, dtype in df.dtypes:
        print(f"{col_name:<40} | {dtype:<20}")
    print("\n")

def run_profiling():
    """
    الدالة الرئيسية لإدارة العملية لـ 35 جدول أو أكثر.
    """
    spark = get_spark_session("Quick_Data_Inspector")
    
    if not os.path.exists(BRONZE_PATH):
        print(f"❌ Error: Bronze path not found at {BRONZE_PATH}")
        return

    # جلب كل المجلدات الموجودة في Bronze
    tables = sorted([d for d in os.listdir(BRONZE_PATH) 
                     if os.path.isdir(os.path.join(BRONZE_PATH, d))])
    
    if not tables:
        print("ℹ️ No data found in Bronze.")
        return

    print(f"🚀 Starting Quick Inspection for {len(tables)} tables...")

    for table in tables:
        try:
            profile_table(spark, table)
        except Exception as e:
            print(f"❌ Error inspecting {table}: {str(e)[:100]}...")

    spark.stop()
    print("\n✅ Quick Inspection Complete.")

if __name__ == "__main__":
    run_profiling()