
# Enterprise Retail Lakehouse Platform 🛒📊

A production-grade, end-to-end Data Lakehouse platform built with **Apache Airflow**, **Apache Spark 3.5**, **Delta Lake**, and **dbt**. The platform models high-volume retail supply chain and sales transactions (50,000+ orders) across physical stores and digital channels using the **Medallion Architecture (Bronze, Silver, Gold)**.

---

## Architecture Overview

```text
[ Data Sources / APIs ]
         │
         ▼ (Airflow Daily Orchestration)
  ┌──────────────┐
  │ Bronze Layer │  Raw ingestion into Delta Lake (append-only + audit metadata)
  └──────┬───────┘
         │
         ▼ (PySpark Distributed Cleaning)
  ┌──────────────┐
  │ Silver Layer │  Deduplicated, typed, cleansed Delta tables (partitioned by year/month)
  └──────┬───────┘
         │
         ▼ (dbt & Spark SQL Marts)
  ┌──────────────┐
  │  Gold Layer  │  Kimball Star Schema (fct_orders, fct_daily_sales, dim_stores, dim_products)
  └──────┬───────┘
         │
         ▼ (Data Quality Gate)
[ Automated Assertion Audit (100% Pass) ]
```
