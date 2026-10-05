Write-Host "=========================================" -ForegroundColor Cyan
Write-Host "STEP 1: Generating Raw Datasets..." -ForegroundColor Cyan
Write-Host "=========================================" -ForegroundColor Cyan
uv run python scripts/generate_retail_data.py

Write-Host "`n=========================================" -ForegroundColor Cyan
Write-Host "STEP 2: Ingesting Raw Data to Bronze..." -ForegroundColor Cyan
Write-Host "=========================================" -ForegroundColor Cyan
docker exec -u 0 -it -e HOME=/tmp -e PYTHONPATH=/opt/spark retail-lakehouse-platform-spark-master-1 /opt/spark/bin/spark-submit --packages io.delta:delta-spark_2.12:3.2.0,io.delta:delta-storage:3.2.0 --conf spark.jars.ivy=/tmp/.ivy --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog /opt/spark/spark/jobs/raw_to_bronze.py

Write-Host "`n=========================================" -ForegroundColor Cyan
Write-Host "STEP 3: Cleansing & Deduplicating to Silver..." -ForegroundColor Cyan
Write-Host "=========================================" -ForegroundColor Cyan
docker exec -u 0 -it -e HOME=/tmp -e PYTHONPATH=/opt/spark retail-lakehouse-platform-spark-master-1 /opt/spark/bin/spark-submit --packages io.delta:delta-spark_2.12:3.2.0,io.delta:delta-storage:3.2.0 --conf spark.jars.ivy=/tmp/.ivy --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog /opt/spark/spark/jobs/bronze_to_silver.py

Write-Host "`n=========================================" -ForegroundColor Cyan
Write-Host "STEP 4: Materializing Gold Kimball Marts..." -ForegroundColor Cyan
Write-Host "=========================================" -ForegroundColor Cyan
docker exec -u 0 -it -e HOME=/tmp -e PYTHONPATH=/opt/spark retail-lakehouse-platform-spark-master-1 /opt/spark/bin/spark-submit --packages io.delta:delta-spark_2.12:3.2.0,io.delta:delta-storage:3.2.0 --conf spark.jars.ivy=/tmp/.ivy --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog /opt/spark/spark/jobs/silver_to_gold.py

Write-Host "`n=========================================" -ForegroundColor Cyan
Write-Host "STEP 5: Running Data Quality Audit..." -ForegroundColor Cyan
Write-Host "=========================================" -ForegroundColor Cyan
docker exec -u 0 -it -e HOME=/tmp -e PYTHONPATH=/opt/spark retail-lakehouse-platform-spark-master-1 /opt/spark/bin/spark-submit --packages io.delta:delta-spark_2.12:3.2.0,io.delta:delta-storage:3.2.0 --conf spark.jars.ivy=/tmp/.ivy --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog /opt/spark/spark/jobs/data_quality_audit.py