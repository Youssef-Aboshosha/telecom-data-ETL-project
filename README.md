# 🚀 Telecom Data Platform: End-to-End Medallion Architecture

### The Ultimate Galaxy Schema Data Pipeline

Built with **PySpark, Delta Lake, Airflow, Docker, Snowflake, and Power BI** ⚡🧊💨🐳❄️📈

---

# 🏗️ System Architecture

![Architecture Diagram](assets/architecture.png)

This project implements a **production-grade Data Engineering pipeline** using the **Medallion Architecture (Bronze → Silver → Gold)** to process **15+ million telecom records**.

The pipeline transforms raw telecom transaction logs into a **highly optimized analytical Galaxy Schema** designed for **executive analytics and predictive modeling**.

### Data Flow

Raw Sources
↓
Bronze Layer (Delta Lake)
↓
Silver Layer (Cleaned & Validated Data)
↓
Gold Layer (Galaxy Schema Warehouse)
↓
Snowflake Data Warehouse
↓
Power BI Dashboards

---

# 💡 Key Skills Demonstrated

* Medallion Architecture Design
* PySpark Distributed Data Processing
* Delta Lake Optimization
* Slowly Changing Dimensions (SCD Type 2)
* Data Warehouse Modeling (Galaxy Schema)
* Apache Airflow Workflow Orchestration
* Docker Containerized Data Platforms
* Snowflake Data Warehouse Integration
* Business Intelligence with Power BI
* Data Quality & Reconciliation Engineering

---

# 🛠️ Tech Stack

| Layer             | Technology                        |
| ----------------- | --------------------------------- |
| Processing Engine | PySpark (Apache Spark 3.5) ⚡      |
| Data Storage      | Delta Lake & Parquet 🧊           |
| Orchestration     | Apache Airflow (Astro Runtime) 💨 |
| Containerization  | Docker 🐳                         |
| Data Warehouse    | Snowflake ❄️                      |
| Visualization     | Power BI 📈                       |
| Programming       | Python 🐍                         |
| Environment       | Astronomer Astro CLI 🚀           |

---

# 📦 Dataset Characteristics

Total Records Processed: **15+ Million**

The platform ingests multiple telecom data sources:

* Call Detail Records (CDR)
* SMS Transaction Logs
* Mobile Data Sessions
* Customer Billing & Payments
* Sales Transactions
* Customer Support Complaints
* Churn Prediction Scores

This dataset supports **operational analytics, financial insights, and customer experience analysis**.

---

# 🧱 Medallion Data Architecture

## 🟤 Bronze Layer — Raw Data

Purpose: **Raw immutable ingestion**

Features:

* Source data stored in **Delta Lake format**
* Schema-on-read ingestion
* Full raw audit history
* ACID transaction support

Input formats include:

* CSV
* Parquet
* JSON logs

---

## ⚪ Silver Layer — Cleaned Data

Purpose: **Data quality enforcement**

Processing steps include:

* Schema validation
* Deduplication
* Null handling
* Data type standardization
* Referential integrity checks

This layer produces **trusted datasets ready for analytics modeling**.

---

## 🟡 Gold Layer — Business Warehouse

Purpose: **Business-ready analytics layer**

The Gold layer implements a **Galaxy Schema** optimized for analytical workloads.

Key features:

* SCD Type 2 dimensions
* Partitioned fact tables
* Z-Ordering optimization
* Surrogate keys
* High-performance joins

Gold tables are automatically synchronized to **Snowflake** for BI consumption.

---

# 📊 Data Modeling — Galaxy Schema

Unlike a simple **Star Schema**, this project implements a **Galaxy Schema** where multiple fact tables share conformed dimensions.

![Galaxy Schema](assets/galaxy_schema.png)

This design enables a **360-degree view of telecom operations**.

---

# 🧩 Dimension Tables

### dim_customer (SCD Type 2)

Tracks customer profile history using **SHA2 hash change detection**.

Features:

* Surrogate keys
* Effective start/end dates
* Historical tracking
* Change detection

---

### dim_plan_catalog

Unified catalog of telecom offerings:

* Mobile plans
* Data add-ons
* SMS bundles
* Roaming packages

---

### dim_geography

Denormalized hierarchy:

City → Region → Country

Supports regional analytics and network ROI analysis.

---

### dim_phone

Links **physical telecom assets to customer subscriptions**.

Tracks:

* SIM
* IMEI
* Device ownership

---

### dim_date

Industry-standard date dimension including:

* Year
* Quarter
* Month
* Week
* Day
* Unknown date record

---

# 📈 Fact Tables

### fact_usage

12M+ telecom activity records including:

* Calls
* SMS
* Data sessions

Metrics:

* Call duration
* Data consumption
* SMS volume

---

### fact_financials

Financial auditing layer tracking:

* Invoice amounts
* Payments
* Revenue streams
* Outstanding balances

---

### fact_sales

Sales analytics including:

* Plan purchases
* Customer acquisitions
* Regional sales performance

---

### fact_customer_experience

Customer sentiment analytics:

* NPS scores
* Complaint frequency
* Churn prediction probabilities

---

# 🚀 Engineering Challenges & Solutions

## 1️⃣ Data Reconciliation — The $2× Revenue Bug

During development, a major issue appeared:

Gold revenue was **double the Silver revenue**.

### Root Cause

Duplicate records in `dim_customer` caused **Cartesian joins**, inflating revenue metrics.

### Solution

Implemented strict uniqueness validation:

df = df.dropDuplicates(["customer_id"])

Then created a reconciliation validation script ensuring:

Gold Revenue = Silver Revenue
Variance = 0%

This ensured **financial reporting accuracy**.

---

## 2️⃣ Performance Optimization

Processing **15M+ records in Docker containers** required optimization.

### Z-Ordering

Applied on:

* customer_sk
* date_key

Benefits:

* Faster joins
* Query pruning
* Reduced scan times

---

### Partitioning Strategy

Fact tables partitioned by:

* year
* month

Benefits:

* Faster Power BI queries
* Reduced Snowflake compute cost
* Efficient data pruning

---

## 3️⃣ Automated Snowflake Synchronization

Gold tables are automatically **pushed to Snowflake**, enabling:

* Immediate BI availability
* Centralized analytics warehouse
* Scalable reporting infrastructure

---

# ⚙️ Pipeline Orchestration (Apache Airflow)

The pipeline is orchestrated by a **Master DAG**.

Execution flow:

Raw Data Ingestion
↓
Bronze & Silver Processing
↓
Base Dimensions
(Date, Geography, Plan Catalog)
↓
Complex Dimensions
(Customer SCD Type 2, Phone)
↓
Fact Tables
(Usage, Sales, Financials, Customer Experience)
↓
Snowflake Warehouse Sync

The DAG is **idempotent**, meaning it can safely re-run without duplicating data.

---

# 📈 Business Intelligence (Power BI)

The Gold layer powers executive dashboards.

![Power BI Dashboard](assets/dashboard.png)

Key analytics include:

### 📊 ARPU

Average Revenue Per User.

### ⚠️ Churn Risk

Identifies customers with:

* High complaint frequency
* High churn probability

### 🌍 Network ROI

Compares **usage volume vs revenue per region**.

---

# 📂 Project Structure

telecom-data-ETL-project
│
├── dags/
│   └── telecom_gold_layer_master.py
│
├── include/
│   ├── transformations/
│   │     bronze_layer.py
│   │     silver_layer.py
│   │     gold_layer.py
│   │
│   └── data/
│         delta_tables/
│
├── assets/
│   architecture.png
│   galaxy_schema.png
│   dashboard.png
│
├── Dockerfile
├── requirements.txt
├── airflow_settings.yaml
└── README.md

---

# ▶️ Running the Project Locally

### Prerequisites

* Docker
* Astronomer CLI

---

### Clone the Repository

git clone https://github.com/Youssef-Aboshosha/telecom-data-ETL-project.git

---

### Start the Platform

astro dev start

---

### Open Airflow UI

http://localhost:8080

---

### Run the Pipeline

Trigger the DAG:

telecom_gold_layer_master

This executes the full **Bronze → Silver → Gold pipeline**.

---

# 👨‍💻 Author

**Youssef Aboshosha**

LinkedIn: *(Add your link)*
Portfolio: *(Add your link)*
GitHub: https://github.com/Youssef-Aboshosha

---

# ⭐ Project Summary

This project demonstrates a **production-grade data engineering system** combining:

* Lakehouse architecture
* Distributed Spark processing
* Automated workflow orchestration
* Enterprise data warehousing
* Business intelligence dashboards

The result is a **fully automated telecom analytics platform capable of processing millions of records efficiently**.
