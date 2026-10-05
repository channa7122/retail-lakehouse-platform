"""Silver to Gold Transformation Job.

Materializes Kimball Star Schema marts (Fact & Dimension Delta tables)
from cleansed Silver layer data.
"""

from pathlib import Path

from pyspark.sql import functions as F

from spark.utils.session import get_spark_session

if Path("/opt/spark/data").exists():
    BASE_DATA_DIR = Path("/opt/spark/data")
else:
    BASE_DATA_DIR = Path("data")

SILVER_PATH = BASE_DATA_DIR / "silver"
GOLD_PATH = BASE_DATA_DIR / "gold"


def build_dimensions(spark) -> None:
    """Build and save dimension tables."""
    print("Materializing Gold Dimensions...")

    # Dim Stores
    stores_df = spark.read.format("delta").load(str(SILVER_PATH / "stores"))
    stores_df.write.format("delta").mode("overwrite").save(str(GOLD_PATH / "dim_stores"))
    print(f"  [OK] Saved {stores_df.count():,} rows to dim_stores")

    # Dim Products
    products_df = spark.read.format("delta").load(str(SILVER_PATH / "products"))
    dim_products = products_df.withColumn(
        "profit_margin_amount", F.round(F.col("retail_price") - F.col("unit_cost"), 2)
    ).withColumn(
        "profit_margin_pct",
        F.round(
            ((F.col("retail_price") - F.col("unit_cost")) / F.col("retail_price")) * 100,
            2,
        ),
    )
    dim_products.write.format("delta").mode("overwrite").save(str(GOLD_PATH / "dim_products"))
    print(f"  [OK] Saved {dim_products.count():,} rows to dim_products")

    # Dim Customers
    cust_df = spark.read.format("delta").load(str(SILVER_PATH / "customers"))
    dim_cust = cust_df.withColumn("full_name", F.concat_ws(" ", F.col("first_name"), F.col("last_name")))
    dim_cust.write.format("delta").mode("overwrite").save(str(GOLD_PATH / "dim_customers"))
    print(f"  [OK] Saved {dim_cust.count():,} rows to dim_customers")


def build_facts(spark) -> None:
    """Build and save fact tables."""
    print("Materializing Gold Facts...")
    orders_df = spark.read.format("delta").load(str(SILVER_PATH / "orders"))
    items_df = spark.read.format("delta").load(str(SILVER_PATH / "order_items"))
    products_df = spark.read.format("delta").load(str(SILVER_PATH / "products"))

    # Fct Orders
    items_summary = items_df.groupBy("order_id").agg(
        F.count("order_item_id").alias("total_items_count"),
        F.sum("quantity").alias("total_quantity"),
        F.round(F.sum("total_price"), 2).alias("gross_revenue"),
        F.round(F.sum("discount_amount"), 2).alias("total_discount"),
    )

    fct_orders = (
        orders_df.join(items_summary, on="order_id", how="left")
        .withColumn("order_date", F.to_date("order_timestamp"))
        .withColumn("net_revenue", F.round(F.col("gross_revenue") - F.col("total_discount"), 2))
    )
    fct_orders.write.format("delta").mode("overwrite").save(str(GOLD_PATH / "fct_orders"))
    print(f"  [OK] Saved {fct_orders.count():,} rows to fct_orders")

    # Fct Daily Sales Aggregation (Disambiguated Join)
    fct_daily_sales = (
        orders_df.filter(F.col("order_status") == "Completed")
        .join(items_df.drop("order_timestamp"), on="order_id")
        .join(products_df, on="product_id")
        .withColumn("sales_date", F.to_date("order_timestamp"))
        .groupBy("sales_date", "store_id", "category")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.sum("quantity").alias("total_units_sold"),
            F.round(F.sum("total_price"), 2).alias("daily_gross_revenue"),
            F.round(F.sum("discount_amount"), 2).alias("daily_discount_amount"),
        )
        .withColumn(
            "daily_net_revenue",
            F.round(F.col("daily_gross_revenue") - F.col("daily_discount_amount"), 2),
        )
    )
    fct_daily_sales.write.format("delta").mode("overwrite").save(str(GOLD_PATH / "fct_daily_sales"))
    print(f"  [OK] Saved {fct_daily_sales.count():,} rows to fct_daily_sales")


def main():
    spark = get_spark_session("Silver-To-Gold-Marts")
    spark.sparkContext.setLogLevel("WARN")

    try:
        build_dimensions(spark)
        build_facts(spark)
        print("Gold layer materialization completed successfully.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
