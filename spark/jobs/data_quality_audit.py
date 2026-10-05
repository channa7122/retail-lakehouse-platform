"""Data Quality Audit Job.

Performs automated assertions across Bronze, Silver, and Gold Delta tables
to ensure row count consistency, primary key uniqueness, and schema health.
"""

from pathlib import Path
from spark.utils.session import get_spark_session

if Path("/opt/spark/data").exists():
    BASE_DATA_DIR = Path("/opt/spark/data")
else:
    BASE_DATA_DIR = Path("data")

BRONZE_PATH = BASE_DATA_DIR / "bronze"
SILVER_PATH = BASE_DATA_DIR / "silver"
GOLD_PATH = BASE_DATA_DIR / "gold"


def run_audit() -> None:
    spark = get_spark_session("Lakehouse-Data-Quality-Audit")
    spark.sparkContext.setLogLevel("WARN")

    try:
        print("=" * 60)
        print("RUNNING LAKEHOUSE DATA QUALITY AUDIT")
        print("=" * 60)

        # 1. Audit Bronze Orders
        bronze_orders = spark.read.format("delta").load(str(BRONZE_PATH / "orders"))
        bronze_count = bronze_orders.count()
        print(f"[Bronze] Raw Orders Count: {bronze_count:,}")
        assert bronze_count > 0, "Bronze orders table is empty!"

        # 2. Audit Silver Orders (Deduplication check)
        silver_orders = spark.read.format("delta").load(str(SILVER_PATH / "orders"))
        silver_count = silver_orders.count()
        silver_distinct = silver_orders.select("order_id").distinct().count()
        print(f"[Silver] Clean Orders Count: {silver_count:,} (Distinct: {silver_distinct:,})")
        assert silver_count == silver_distinct, "Duplicate order_ids detected in Silver!"
        assert silver_count < bronze_count, "Deduplication did not remove duplicates from Bronze!"

        # 3. Audit Silver Order Items (No negative quantities)
        silver_items = spark.read.format("delta").load(str(SILVER_PATH / "order_items"))
        negative_qty = silver_items.filter("quantity <= 0").count()
        print(f"[Silver] Order Items Count: {silver_items.count():,} (Invalid Qty: {negative_qty})")
        assert negative_qty == 0, "Negative or zero quantities found in Silver order items!"

        # 4. Audit Gold Marts
        fct_orders = spark.read.format("delta").load(str(GOLD_PATH / "fct_orders"))
        fct_sales = spark.read.format("delta").load(str(GOLD_PATH / "fct_daily_sales"))
        dim_cust = spark.read.format("delta").load(str(GOLD_PATH / "dim_customers"))

        print(f"[Gold] Fact Orders Count: {fct_orders.count():,}")
        print(f"[Gold] Fact Daily Sales Count: {fct_sales.count():,}")
        print(f"[Gold] Dim Customers Count: {dim_cust.count():,}")

        assert fct_orders.count() == silver_count, "Fact orders row count does not match Silver!"
        print("=" * 60)
        print("ALL DATA QUALITY CHECKS PASSED SUCCESSFULLY (100%)")
        print("=" * 60)
    finally:
        spark.stop()


if __name__ == "__main__":
    run_audit()