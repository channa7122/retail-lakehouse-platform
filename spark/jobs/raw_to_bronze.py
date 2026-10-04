"""Raw to Bronze Ingestion Job.

Reads raw CSV and JSON files from raw data directory, enriches them with audit columns
(_ingested_at, _source_file), and appends them to Bronze Delta Lake tables.
"""

from pathlib import Path
from pyspark.sql import functions as F
from spark.utils.session import get_spark_session

# Automatically detect if running inside Docker container or local host
if Path("/opt/spark/data").exists():
    BASE_DATA_DIR = Path("/opt/spark/data")
else:
    BASE_DATA_DIR = Path("data")

RAW_PATH = BASE_DATA_DIR / "raw"
BRONZE_PATH = BASE_DATA_DIR / "bronze"


def ingest_csv_to_bronze(spark, filename: str, table_name: str) -> None:
    """Ingest a CSV source file into an append-only Bronze Delta table."""
    file_path = str(RAW_PATH / filename)
    target_path = str(BRONZE_PATH / table_name)

    print(f"Ingesting {filename} -> Bronze ({table_name})...")
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(file_path)
    )

    enriched_df = df.withColumn(
        "_ingested_at", F.current_timestamp()
    ).withColumn("_source_file", F.lit(filename))

    enriched_df.write.format("delta").mode("append").save(target_path)
    print(f"  [OK] Successfully saved {enriched_df.count():,} rows to {target_path}")


def ingest_orders_to_bronze(spark) -> None:
    """Ingest nested orders JSON into Bronze Delta table."""
    filename = "orders.json"
    file_path = str(RAW_PATH / filename)
    target_path = str(BRONZE_PATH / "orders")

    print(f"Ingesting {filename} -> Bronze (orders)...")
    df = spark.read.option("multiline", "true").json(file_path)

    enriched_df = df.withColumn(
        "_ingested_at", F.current_timestamp()
    ).withColumn("_source_file", F.lit(filename))

    enriched_df.write.format("delta").mode("append").save(target_path)
    print(f"  [OK] Successfully saved {enriched_df.count():,} orders to {target_path}")


def main():
    spark = get_spark_session("Raw-To-Bronze-Ingestion")
    spark.sparkContext.setLogLevel("WARN")

    try:
        ingest_csv_to_bronze(spark, "stores.csv", "stores")
        ingest_csv_to_bronze(spark, "products.csv", "products")
        ingest_csv_to_bronze(spark, "customers.csv", "customers")
        ingest_orders_to_bronze(spark)
        print("Bronze ingestion pipeline completed successfully.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()