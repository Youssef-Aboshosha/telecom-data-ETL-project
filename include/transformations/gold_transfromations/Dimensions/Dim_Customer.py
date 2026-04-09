from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

from include.transformations.spark_utils import (
    get_spark_session,
    SILVER_PATH,
    GOLD_PATH
)


def build_dim_customer():

    spark = get_spark_session("gold_dim_customer")

    print("👤 Building Dim_Customer (SCD Type 2)...")

    # ------------------------------------------------
    # 1 Load Silver Tables
    # ------------------------------------------------

    customers = spark.read.format("delta").load(f"{SILVER_PATH}/customers")
    addresses = spark.read.format("delta").load(f"{SILVER_PATH}/customer_addresses")
    segments = spark.read.format("delta").load(f"{SILVER_PATH}/customer_segments")
    cities = spark.read.format("delta").load(f"{SILVER_PATH}/cities")
    countries = spark.read.format("delta").load(f"{SILVER_PATH}/countries")

    # ------------------------------------------------
    # 2 Denormalize (Build Wide Dimension)
    # ------------------------------------------------

    enriched = (
        customers.alias("c")
        .join(addresses.alias("a"), "customer_id", "left")
        .join(segments.alias("s"), "segment_id", "left")
        .join(cities.alias("ci"), "city_id", "left")
        .join(countries.alias("co"), "country_id", "left")
        .select(
            F.col("c.customer_id"),

            F.concat_ws(
                " ",
                F.col("c.first_name"),
                F.col("c.last_name")
            ).alias("full_name"),

            F.col("c.email"),
            F.col("c.phone"),
            F.col("s.segment_name"),
            F.col("a.address_text"),
            F.col("ci.city_name"),
            F.col("co.country_name"),
            F.col("c.status")
        )
    )

    # ------------------------------------------------
    # 3 Change Detection Hash
    # ------------------------------------------------

    enriched = enriched.withColumn(

        "row_hash",

        F.sha2(

            F.concat_ws(
                "||",
                "customer_id",
                "full_name",
                "email",
                "phone",
                "segment_name",
                "address_text",
                "city_name",
                "country_name",
                "status"
            ),

            256
        )
    )

    # ------------------------------------------------
    # 4 Add SCD Metadata
    # ------------------------------------------------

    staged = (
        enriched
        .withColumn("customer_sk", F.expr("uuid()"))
        .withColumn("valid_from", F.current_timestamp())
        .withColumn("valid_to", F.lit(None).cast(TimestampType()))
        .withColumn("is_current", F.lit(True))
    )

    target_path = f"{GOLD_PATH}/dim_customer"

    # ------------------------------------------------
    # 5 Initial Load
    # ------------------------------------------------

    if not DeltaTable.isDeltaTable(spark, target_path):

        staged.write \
            .format("delta") \
            .mode("overwrite") \
            .save(target_path)

        print("✅ Initial Load Completed")

    else:

        print("🔄 Running SCD Type 2 Merge")

        dim_table = DeltaTable.forPath(spark, target_path)

        dim_table.alias("target").merge(

            staged.alias("source"),

            "target.customer_id = source.customer_id AND target.is_current = true"

        ) \
        .whenMatchedUpdate(

            condition="target.row_hash <> source.row_hash",

            set={
                "valid_to": "current_timestamp()",
                "is_current": "false"
            }

        ) \
        .whenNotMatchedInsert(

            values={

                "customer_sk": "source.customer_sk",
                "customer_id": "source.customer_id",
                "full_name": "source.full_name",
                "email": "source.email",
                "phone": "source.phone",
                "segment_name": "source.segment_name",
                "address_text": "source.address_text",
                "city_name": "source.city_name",
                "country_name": "source.country_name",
                "status": "source.status",
                "row_hash": "source.row_hash",
                "valid_from": "source.valid_from",
                "valid_to": "source.valid_to",
                "is_current": "source.is_current"
            }

        ).execute()

        print("✅ SCD Type 2 Merge Completed")

    spark.stop()


if __name__ == "__main__":
    build_dim_customer()