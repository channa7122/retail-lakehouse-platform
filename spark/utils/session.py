"""Spark Session Builder for Delta Lake.

Provides a unified SparkSession configured with Delta Lake extensions
and local directory catalog settings.
"""

from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "Retail-Lakehouse-Engine") -> SparkSession:
    """Build and return a SparkSession configured with Delta Lake."""
    builder = (
        SparkSession.Builder()
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", "data/warehouse")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
    )
    return builder.getOrCreate()