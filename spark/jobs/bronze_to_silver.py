"""Bronze to Silver Transformation Job.

Applies enterprise cleaning rules: deduplication, schema casting, date parsing,
filtering anomalies, flattening nested order items, and partitioning by year/month.
"""

from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
)

from spark.utils.session import get_spark_session

if Path("/opt/spark/data").exists():
    BASE_DATA_DIR = Path("/opt/spark/data")
else:
    BASE_DATA_DIR = Path("data")

BRONZE_PATH = BASE_DATA_DIR / "bronze"
SILVER_PATH = BASE_DATA_DIR / "silver"


def clean_stores(spark) -> None:
    """Clean stores: trim whitespaces, standardize casing, filter nulls."""
    print("Processing Bronze Stores -> Silver Stores...")
    df = spark.read.format("delta").load(str(BRONZE_PATH / "stores"))

    cleaned_df = (
        df.dropDuplicates(["store_id"])
        .withColumn("store_name", F.trim(F.initcap(F.col("store_name"))))
        .withColumn("region", F.trim(F.col("region")))
        .withColumn("store_type", F.trim(F.col("store_type")))
        .withColumn("opened_date", F.to_date(F.col("opened_date"), "yyyy-MM-dd"))
        .withColumn("_transformed_at", F.current_timestamp())
    )

    target_path = str(SILVER_PATH / "stores")
    cleaned_df.write.format("delta").mode("overwrite").save(target_path)
    print(f"  [OK] Saved {cleaned_df.count():,} cleaned stores to {target_path}")


def clean_products(spark) -> None:
    """Clean products: handle null unit costs, normalize category casing."""
    print("Processing Bronze Products -> Silver Products...")
    df = spark.read.format("delta").load(str(BRONZE_PATH / "products"))

    cleaned_df = (
        df.dropDuplicates(["product_id"])
        .withColumn("category", F.initcap(F.trim(F.col("category"))))
        .withColumn("product_name", F.trim(F.col("product_name")))
        .withColumn(
            "unit_cost",
            F.when(
                F.col("unit_cost").isNull(),
                F.round(F.col("retail_price") * 0.65, 2),
            ).otherwise(F.col("unit_cost")),
        )
        .withColumn("_transformed_at", F.current_timestamp())
    )

    target_path = str(SILVER_PATH / "products")
    cleaned_df.write.format("delta").mode("overwrite").save(target_path)
    print(f"  [OK] Saved {cleaned_df.count():,} cleaned products to {target_path}")


def clean_customers(spark) -> None:
    """Clean customers: standardize emails, default missing loyalty tiers."""
    print("Processing Bronze Customers -> Silver Customers...")
    df = spark.read.format("delta").load(str(BRONZE_PATH / "customers"))

    cleaned_df = (
        df.dropDuplicates(["customer_id"])
        .withColumn("email", F.lower(F.trim(F.col("email"))))
        .withColumn(
            "loyalty_tier",
            F.coalesce(F.col("loyalty_tier"), F.lit("Standard")),
        )
        .withColumn("created_at", F.to_date(F.col("created_at"), "yyyy-MM-dd"))
        .withColumn("_transformed_at", F.current_timestamp())
    )

    target_path = str(SILVER_PATH / "customers")
    cleaned_df.write.format("delta").mode("overwrite").save(target_path)
    print(f"  [OK] Saved {cleaned_df.count():,} cleaned customers to {target_path}")


def clean_orders_and_items(spark) -> None:
    """Process orders: deduplicate, parse mixed date formats, filter anomalies, and flatten items."""
    print("Processing Bronze Orders -> Silver Orders & Order Items...")
    df = spark.read.format("delta").load(str(BRONZE_PATH / "orders"))

    # 1. Deduplicate orders by order_id
    deduped_orders = df.dropDuplicates(["order_id"])

    # 2. Parse mixed date formats (ISO, YYYY/MM/DD, DD-MM-YYYY)
    parsed_orders = deduped_orders.withColumn(
        "order_timestamp",
        F.coalesce(
            F.to_timestamp(F.col("order_timestamp"), "yyyy-MM-dd'T'HH:mm:ss"),
            F.to_timestamp(F.col("order_timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"),
            F.to_timestamp(F.col("order_timestamp"), "yyyy/MM/dd HH:mm:ss"),
            F.to_timestamp(F.col("order_timestamp"), "dd-MM-yyyy"),
        ),
    ).filter(F.col("order_timestamp").isNotNull())

    # 3. Add partition columns
    silver_orders = (
        parsed_orders.withColumn("order_year", F.year("order_timestamp"))
        .withColumn("order_month", F.month("order_timestamp"))
        .withColumn("_transformed_at", F.current_timestamp())
    )

    orders_target = str(SILVER_PATH / "orders")
    # Save orders partitioned by year and month
    (
        silver_orders.drop("items")
        .write.format("delta")
        .mode("overwrite")
        .partitionBy("order_year", "order_month")
        .save(orders_target)
    )
    print(f"  [OK] Saved {silver_orders.count():,} Silver orders (partitioned) to {orders_target}")

    # 4. Explode nested items into relational Silver table: order_items
    silver_items = (
        silver_orders.select(
            "order_id",
            "order_timestamp",
            F.explode("items").alias("item"),
        )
        .select(
            F.col("item.order_item_id").alias("order_item_id"),
            F.col("order_id"),
            F.col("item.product_id").alias("product_id"),
            F.col("item.quantity").cast(IntegerType()).alias("quantity"),
            F.col("item.unit_price").cast(DoubleType()).alias("unit_price"),
            F.col("item.discount_amount").cast(DoubleType()).alias("discount_amount"),
            F.col("order_timestamp"),
        )
        # Quality rule: remove negative or zero quantity anomalies
        .filter(F.col("quantity") > 0)
        .withColumn(
            "total_price",
            F.round(
                (F.col("quantity") * F.col("unit_price")) - F.col("discount_amount"),
                2,
            ),
        )
        .withColumn("_transformed_at", F.current_timestamp())
    )

    items_target = str(SILVER_PATH / "order_items")
    silver_items.write.format("delta").mode("overwrite").save(items_target)
    print(f"  [OK] Saved {silver_items.count():,} cleaned order items to {items_target}")


def main():
    spark = get_spark_session("Bronze-To-Silver-Transformation")
    spark.sparkContext.setLogLevel("WARN")

    try:
        clean_stores(spark)
        clean_products(spark)
        clean_customers(spark)
        clean_orders_and_items(spark)
        print("Silver transformation pipeline completed successfully.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
