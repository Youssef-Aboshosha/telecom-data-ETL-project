# 🚀 Telecom Data Platform: End-to-End Medallion Architecture

## The Ultimate Galaxy Schema Data Pipeline
Built with PySpark, Delta Lake, Airflow, Docker, Snowflake, and Power BI 🌐🏢📊

---

# 🏗️ Architecture Overview

This project implements a **production-ready Medallion Architecture (Bronze → Silver → Gold)** to process **15+ million telecom records**.

The pipeline transforms raw transactional telecom data into a **highly optimized Galaxy Schema** designed for **executive analytics and predictive modeling**.

##  System Architecture

![Architecture Diagram](assets/architecture.png)

The pipeline follows a Medallion architecture:

Raw Sources → Bronze → Silver → Gold → Snowflake → Power BI

### Data Flow

Raw Sources → Bronze → Silver → Gold → Snowflake → Power BI

**Raw Layer**
- Ingestion of disparate sources into **Parquet and CSV**

**Bronze Layer**
- Raw ingestion into **Delta Lake**
- Ensures **ACID compliance and auditability**

**Silver Layer**
- Data cleaning
- Schema enforcement
- Deduplication

**Gold Layer**
- Business modeling using a **Galaxy Schema**
- SCD Type 2 dimensions
- Partitioning & Z-Ordering

**Warehouse & BI**
- Gold data pushed automatically to **Snowflake**
- Consumed by **Power BI dashboards**

---

# 🛠️ Tech Stack

| Layer | Technology |
|-----|------|
| Processing | PySpark (Apache Spark 3.5) ⚡ |
| Storage | Delta Lake & Parquet 🧊 |
| Orchestration | Apache Airflow 💨 |
| Containerization | Docker 🐳 |
| Data Warehouse | Snowflake ❄️ |
| Visualization | Power BI 📈 |
| Language | Python 🐍 |
| Environment | Astronomer Astro CLI 🚀 |

---

# 📊 Data Modeling – Galaxy Schema

Unlike a traditional **Star Schema**, this platform implements a **Galaxy Schema**.

Multiple **Fact Tables** share **Conformed Dimensions**, providing a **360° view of telecom operations**.

---

# 🧩 Dimensions

### dim_customer (SCD Type 2)
Tracks historical customer changes using **SHA2 hash change detection**.

### dim_plan_catalog
Unified catalog for telecom **plans and add-ons**.

### dim_geography
Denormalized **city → country hierarchy**.

### dim_phone
Maps **SIM / IMEI devices to customer subscriptions**.

### dim_date
Industry standard **date dimension with unknown record logic**.

---

# 📈 Fact Tables

### fact_usage
12M+ telecom usage records:
- Calls
- SMS
- Data

### fact_financials
Billing audit:
- Invoices
- Payments
- Revenue

### fact_sales
Sales performance tracking:
- Plan purchases
- Revenue by region

### fact_customer_experience
Customer sentiment:
- NPS
- Complaints
- Churn prediction

---

# 🚀 Engineering Challenges & Solutions

## 1️⃣ Data Reconciliation — The $2× Revenue Bug

**Problem**

Gold layer revenue was **double the Silver layer revenue**.

**Root Cause**

Duplicate records in `dim_customer` caused **Cartesian joins**.

**Solution**

Implemented strict uniqueness validation:

dropDuplicates(["customer_id"])

Added a reconciliation script ensuring **0% variance between layers**.

---

## 2️⃣ Performance Optimization

Processing **15M+ records inside Docker** required aggressive tuning.

### Z-Ordering
Applied on:

- `customer_sk`
- `date_key`

Improves query pruning and join performance.

### Partitioning

Partitioned by:
year / month


Benefits:

- Faster Power BI queries
- Reduced Snowflake credit usage

---

## 3️⃣ Automated Snowflake Sync

Gold tables automatically **push to Snowflake** using the connector.

This ensures **real-time availability for dashboards**.

---

# ⚙️ Pipeline Orchestration (Airflow)

A **Master DAG** orchestrates the pipeline.

Execution Flow:

Raw Ingestion
↓
Bronze + Silver Processing
↓
Base Dimensions
(Date, Geography, Plans)
↓
SCD Dimensions
(Customer, Phone)
↓
Fact Tables
(Usage, Sales, Financials, CX)


The DAG is **idempotent and dependency-driven**.

---

# 📈 Business Insights (Power BI)

The Gold layer powers executive dashboards:

### ARPU
Average Revenue Per User

### Churn Risk
Customers with high complaint frequency + churn probability.

### Network ROI
Comparing **usage volume vs revenue per region**.

---

# ▶️ Running the Project Locally

Requirements:

- Docker
- Astronomer CLI

Clone the repository:

git clone <repo>

Start the platform:
astro dev start

Open Airflow:
http://localhost:8080


Trigger the DAG:
telecom_gold_layer_master


---

# 👨‍💻 Author

**Youssef [Last Name]**

LinkedIn: [Link]  
Portfolio: [Link]  
GitHub: [Link]
