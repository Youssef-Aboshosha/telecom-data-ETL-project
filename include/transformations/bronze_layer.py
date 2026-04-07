import os
import shutil
import psycopg2
from datetime import datetime
from pyspark.sql import functions as F
from include.transformations.spark_utils import (
    get_spark_session, LANDING_PATH, BRONZE_PATH, load_pipeline_config
)

# 1. POSTGRES WATERMARK MANAGER (Refined)
def get_pg_connection():
    return psycopg2.connect(
        host="postgres", database="postgres", user="postgres", password="postgres", port="5432"
    )

def init_watermark_db():
    conn = get_pg_connection()
    curr = conn.cursor()
    curr.execute("""
        CREATE TABLE IF NOT EXISTS ingestion_watermark (
            table_name VARCHAR(255) PRIMARY KEY,
            last_file VARCHAR(512),
            last_value TIMESTAMP,
            last_ingested_at TIMESTAMP,
            load_type VARCHAR(50),
            status VARCHAR(50)
        )
    """)
    conn.commit()
    curr.close()
    conn.close()

def update_watermark(table_name, file_name, watermark_value, load_type, status):
    conn = get_pg_connection()
    curr = conn.cursor()
    
    # Senior Logic: Convert BigInt/Long to Timestamp for Postgres if it's not None
    # We use TO_TIMESTAMP() in SQL to handle the conversion safely
    query = """
        INSERT INTO ingestion_watermark (table_name, last_file, last_value, last_ingested_at, load_type, status)
        VALUES (%s, %s, CASE WHEN %s::text IS NOT NULL THEN TO_TIMESTAMP(%s::double precision / 1000000) ELSE NULL END, %s, %s, %s)
        ON CONFLICT (table_name) 
        DO UPDATE SET 
            last_file = EXCLUDED.last_file,
            last_value = COALESCE(EXCLUDED.last_value, ingestion_watermark.last_value),
            last_ingested_at = EXCLUDED.last_ingested_at,
            status = EXCLUDED.status;
    """
    # We pass watermark_value twice: one for the check and one for the conversion
    curr.execute(query, (table_name, file_name, watermark_value, watermark_value, datetime.now(), load_type, status))
    conn.commit()
    curr.close()
    conn.close()

def get_last_watermark(table_name):
    conn = get_pg_connection()
    curr = conn.cursor()
    curr.execute("SELECT last_value FROM ingestion_watermark WHERE table_name = %s", (table_name,))
    res = curr.fetchone()
    curr.close()
    conn.close()
    return res[0] if res else None

# 2. RUN PIPELINE (The Full Execution Logic)
def run_bronze_pipeline():
    print("🚀 Engine Started")
    spark = get_spark_session("Bronze_Execution")
    init_watermark_db()
    full_config = load_pipeline_config()

    ARCHIVE_PATH = os.path.join(os.path.dirname(LANDING_PATH), "archive")
    os.makedirs(ARCHIVE_PATH, exist_ok=True)

    print(f"📂 Landing path: {LANDING_PATH}")
    all_files = [f for f in os.listdir(LANDING_PATH) if f.endswith(".parquet")]
    print(f"📁 Files detected: {len(all_files)}")

    for file_name in all_files:
        print(f"\n➡️ Processing file: {file_name}")
        table_name = file_name.replace(".parquet", "")
        t_conf = full_config.get('tables', {}).get(table_name, {"load_type": "FULL"})

        input_path = os.path.join(LANDING_PATH, file_name)
        output_path = os.path.join(BRONZE_PATH, table_name)

        try:
            print(f"🔥 Reading file...")
            df = spark.read.parquet(input_path)
            print(f"📊 Count BEFORE filter: {df.count()}")

            batch_id = datetime.now().strftime("%Y%m%d%H%M")
            df = df.withColumn("_ingested_at", F.current_timestamp()) \
                   .withColumn("_batch_id", F.lit(batch_id)) \
                   .withColumn("_source_file", F.lit(file_name))

            new_watermark_to_save = None

            if t_conf["load_type"] == "INCREMENTAL":
                w_col = t_conf["watermark_col"]
                last_v = get_last_watermark(table_name)
                print(f"⚙️ INCREMENTAL mode | Watermark: {last_v}")

                if last_v:
                    df = df.filter(F.col(w_col) > F.lit(last_v))
                print(f"📊 Count AFTER filter: {df.count()}")

                if df.limit(1).count() > 0:
                    df.write.mode("append").parquet(output_path)
                    new_watermark_to_save = df.agg(F.max(w_col)).collect()[0][0]
                    print("💾 Data written (append)")
                else:
                    print(f"⏭️ {table_name}: No new data.")
            else:
                # FULL LOAD
                print("⚙️ FULL LOAD mode")
                df.write.mode("overwrite").parquet(output_path)
                print("💾 Data written (overwrite)")

            # Update watermark and archive
            update_watermark(table_name, file_name, new_watermark_to_save, t_conf["load_type"], "SUCCESS")
            shutil.move(input_path, os.path.join(ARCHIVE_PATH, f"{batch_id}_{file_name}"))
            print(f"✅ {table_name} processed successfully and archived.")

        except Exception as e:
            print(f"❌ Error processing {table_name}: {e}")
            update_watermark(table_name, file_name, None, t_conf["load_type"], f"FAILED: {str(e)[:50]}")

    spark.stop()
    print("🛑 Engine Stopped")

if __name__ == "__main__":
    run_bronze_pipeline()