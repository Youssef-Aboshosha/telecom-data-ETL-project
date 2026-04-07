import os
import sys
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, FloatType

# إضافة المسار لضمان عمل الـ Imports داخل الحاوية
sys.path.append("/usr/local/airflow")

from include.transformations.spark_utils import get_spark_session, BRONZE_PATH

def profile_table(spark, table_name):
    """
    يقوم بتحليل جودة البيانات لكل جدول واستخراج الإحصائيات الحيوية وعرض عينة من البيانات.
    """
    file_path = f"{BRONZE_PATH}/{table_name}"
    
    # التأكد من أن المسار يحتوي على بيانات فعلاً
    df = spark.read.parquet(file_path)
    total_rows = df.count()
    
    if total_rows == 0:
        print(f"⚠️ Table {table_name} is empty. Skipping profile.")
        return

    print(f"\n📊 Profiling Table: {table_name} ({total_rows:,} rows)")
    
    # --- New Feature: Data Preview (First 5 records) ---
    print("\n👀 Data Preview (First 5 records):")
    df.show(5, truncate=False)
    # --------------------------------------------------

    print("-" * 130)
    print(f"{'Column Name':<30} | {'Type':<10} | {'Nulls':<10} | {'Null%':<8} | {'Distinct':<10} | {'Min / Max'}")
    print("-" * 130)

    for col_name, dtype in df.dtypes:
        # 1. ذكاء التعامل مع الأنواع: فحص الـ NaN فقط للأعمدة الرقمية لتجنب الـ Crash
        is_numeric = dtype in ['double', 'float']
        
        null_cond = F.col(col_name).isNull()
        if is_numeric:
            null_cond = null_cond | F.isnan(F.col(col_name))

        # 2. عملية تجميع الإحصائيات في خطوة واحدة (Optimization)
        stats = df.select(
            F.count(F.when(null_cond, col_name)).alias("null_count"),
            F.countDistinct(col_name).alias("distinct_count"),
            F.min(col_name).alias("min_val"),
            F.max(col_name).alias("max_val")
        ).collect()[0]

        null_count = stats["null_count"]
        null_pct = round((null_count / total_rows) * 100, 2)
        distinct = stats["distinct_count"]
        
        # تنسيق الـ Min/Max بشكل نظيف
        min_max = f"{str(stats['min_val'])[:20]} / {str(stats['max_val'])[:20]}"

        print(f"{col_name:<30} | {dtype:<10} | {null_count:<10} | {null_pct:<8} | {distinct:<10} | {min_max}")

def run_profiling():
    """
    الدالة الرئيسية لإدارة عملية الـ Profiling لكل الجداول المتاحة.
    """
    spark = get_spark_session("Data_Profiler_Pro")
    
    if not os.path.exists(BRONZE_PATH):
        print(f"❌ Error: Bronze path not found at {BRONZE_PATH}")
        return

    # 3. تلقائية الاكتشاف: جلب كل الفولدرات الموجودة في Bronze
    tables = sorted([d for d in os.listdir(BRONZE_PATH) 
                     if os.path.isdir(os.path.join(BRONZE_PATH, d))])
    
    if not tables:
        print("ℹ️ No data found in Bronze to profile.")
        return

    print(f"🚀 Starting Quality Audit for {len(tables)} tables...")

    for table in tables:
        try:
            profile_table(spark, table)
        except Exception as e:
            print(f"❌ Error profiling {table}: {str(e)[:100]}...")

    spark.stop()
    print("\n✅ Data Profiling Audit Complete.")

if __name__ == "__main__":
    run_profiling()