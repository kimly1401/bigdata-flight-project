# Big Data Flight Project  
**End-to-End Big Data Pipeline with Kafka, Spark, Airflow, SQL Server & Metabase**

---

## Project Overview

This project implements a **complete Big Data pipeline** for processing flight data, from raw CSV files to analytics dashboards.

The system is designed following **modern Data Engineering architecture**, including:
- Streaming ingestion with **Apache Kafka**
- Distributed processing with **Apache Spark**
- Workflow orchestration with **Apache Airflow**
- Data warehouse storage in **SQL Server**
- Visualization with **Metabase**

---

## System Architecture
CSV
↓
Kafka (Batch Producer)
↓
Spark Bronze Layer
↓
Spark Silver Layer
↓
Machine Learning Training (Spark MLlib)
↓
SQL Server (Gold Layer)
↓
Metabase Dashboard


---

## Technology Stack

| Layer | Technology |
|---|---|
| Ingestion | Apache Kafka |
| Processing | Apache Spark |
| Orchestration | Apache Airflow |
| Storage (Gold) | SQL Server |
| Visualization | Metabase |
| Containerization | Docker & Docker Compose |

---

## Project Structure

bigdata-flight-project/
├── airflow/
│ └── dags/
│ └── flights_full_pipeline.py
├── kafka/
│ └── producer_full_batch.py
├── spark/
│ ├── bronze_kafka_to_parquet_full.py
│ ├── silver_full_etl.py
│ ├── train_models.py
│ └── silver_to_sql.py
├── jars/
│ └── mssql-jdbc-12.6.1.jre11.jar
├── warehouse/
│ └── init.sql
├── docker-compose.yml
└── README.md


---

## 🔄 Data Pipeline Explanation

### 1️⃣ Data Ingestion (Kafka)
- Flight data is read from CSV files
- A Kafka batch producer sends data to a Kafka topic

**File:**  
`kafka/producer_full_batch.py`

---

### 2️⃣ Bronze Layer (Spark)
- Spark consumes data from Kafka
- Raw data is stored in Parquet format (Bronze layer)

**File:**  
`spark/bronze_kafka_to_parquet_full.py`

---

### 3️⃣ Silver Layer (Spark)
- Data cleaning and transformation
- Schema standardization and quality checks

**File:**  
`spark/silver_full_etl.py`

---

### 4️⃣ Machine Learning Training
- Train ML models using Spark MLlib on Silver data
- Model training is part of the pipeline

**File:**  
`spark/train_models.py`

---

### 5️⃣ Gold Layer – Data Warehouse
- Cleaned data is loaded into SQL Server
- Optimized for analytics and BI queries

**File:**  
`spark/silver_to_sql.py`

---

### Visualization
- Metabase connects to SQL Server
- Dashboards visualize flight statistics and insights

---

## Workflow Orchestration with Airflow

Apache Airflow is used to **orchestrate the entire pipeline**, ensuring correct execution order and fault tolerance.

### Airflow DAG
**File:**  
`airflow/dags/flights_full_pipeline.py`

### DAG Flow
http://localhost:8080
Trigger the DAG: flights_full_pipeline

