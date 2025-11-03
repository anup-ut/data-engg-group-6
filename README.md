 # 🧠  Mapping Initial Status Payments

## 📘 Project Overview

### **Objective**

The goal of this project is to improve the **observability of initiated payments** for **LHV Paytech**.

When an e-shop customer clicks “Make Payment,” the payment journey starts through the Paytech Payment Gateway and provider services. However, tracking these initiated payments has been challenging due to fragmented data sources and poor mapping between initiated and finalized payments.

This project aims to **join the IGW initial payment data with the main DataLake database** to analyze:

* Why initiated payments fail to progress,
* Which payment methods perform best or worst, and
* How to improve the customer payment experience.

---

## 👥 Stakeholders

| Stakeholder          | Benefit                                                                |
| -------------------- | ---------------------------------------------------------------------- |
| **Data Analysts**    | Understand customer behavior and payment patterns                      |
| **System Analysts**  | Identify bugs or inefficiencies in payment methods                     |
| **Customer Support** | Quickly assess the health of payment methods during customer inquiries |

---

## 📊 Key Metrics (KPIs)

1. **≥90%** of initiated payments matched with corresponding real payment records.
2. **Dashboard coverage:** 100% overview of initiated vs. completed/abandoned payments by method.

---

## 💡 Business Questions Answered

* How many initiated payments never reach a payment provider?
* How many initiated payments reach a provider but are abandoned?
* What is the **abandonment rate** by payment method?
* Do some initiated payments finalize using **different methods**?
* Which providers or methods show the **highest non-completion rate**?
* Are there **time-based patterns** (e.g., by day or hour) affecting completion rates?

---

## ⚙️ Tooling

| Purpose                     | Tool                        |
| --------------------------- | --------------------------- |
| Data Storage                | **Apache Iceberg** (AWS S3) |
| Data Processing / Warehouse | **ClickHouse**              |
| ETL                         | **AWS Glue**                |
| Visualization               | **Apache Superset**         |
| (Optional) Governance       | **OpenMetadata**            |

---

## 🧱 Data Architecture
![architecture diagram](https://github.com/user-attachments/assets/ba6a8007-5d32-4fda-8c03-ba2edd1f1d3a)

### **Data Flow**

1. **Source:** Paytech Datalake databases
2. **Ingestion & Pre-processing:** AWS Glue batch jobs
3. **Storage:** S3 → Iceberg tables
4. **Warehouse:** ClickHouse
5. **Reporting:** Apache Superset

**Batch frequency:**

* Weekly for staging datasets
* Daily for production datasets

---

### **Data Quality Checks**

* **Uniqueness:** `id` in InitialPayments must be unique
* **Nulls:** `reference` in Payments cannot be null
* **Timestamp logic:** `created_at` < `updated_at`

---
### **Instructions to run**
1. Write in docker CLI: docker compose up -d
2. Wait the start of each services:
[localhost:8080](http://localhost:8080) for airflow webserver, [localhost:8123](http://localhost:8123/) for clickhouse and [localhost:8081](http://localhost:8081/) for MongoDB. (It can take up to 1 minutes)
3. Log in into Airflow and MongoDB. (Airflow's username and passwords:airflow and MongoDB's username is 'admin' and password is 'password')
4. In Airflow webserver, we will see three separate DAGS: ingestion_pipeline, silver_transformations_pipeline and gold_layer_pipeline.
   * By running ingestion_pipeline, we start three operations: ingestion of link_transactions csv files into MongoDB and then ingestion from MongoDB to ClickHouse bronze layer; payment csv files into ClickHouse bronze layer; and merchants to ClickHouse bronze layer.
   * By running silver_transformations_pipeline, we start transformation and data quality check of three tables(link_transactions, payments and merchants) and ingestion them in silver layer.
   * By running gold_layer_pipeline, we created analytical models by creating dim_date, dim_merchants, dim_payment_state, dim_payment_method and fact_transactions.
5. As three dags files are responsible each layer data ingestion, we should wait the end of one to run another. The workflow for running dags:
run ingestion_dag and wait for its finish* $\quad\rightarrow$ run silver_transformation_pipeline and wait for its finish* $\quad\rightarrow$ run gold_layer_pipeline and wait its finish*.

P.S. As our data took from October 01.10.2025, we put start time for dags as October 1, 2025 so it will have all our data.

*wait until the finish means that last run should show previous day. 

### **DAG's Graph**

<img width="800" height="300" alt="image" src="./docs/images/dag1 (1).png" />

ingestion_pipeline DAG's Graph

<img width="800" height="300" alt="image" src="./docs/images/dag1 (2).png" />

silver_transformations_pipeline DAG's Graph

<img width="800" height="300" alt="image" src="./docs/images/dag1 (3).png" />

gold_layer_pipeline DAG's Graph

### **Analytical Queries**

1. How many initiated payments never reach a payment service provider?

<img width="800" height="336" alt="image" src="https://github.com/user-attachments/assets/f875ad30-f6e1-4084-b083-c844b3547a0f" />

2. How many initiated payments reach a provider but are abandoned before completion?
   
<img width="800" height="343" alt="image" src="https://github.com/user-attachments/assets/1a18a307-d5e0-4420-9ec3-ed9345f99535" />

3. What is the abandonment rate by payment method?
   
<img width="800" height="404" alt="image" src="https://github.com/user-attachments/assets/445f8768-8563-4aaf-a3a3-b34f1795426e" />

4. Which payment methods or providers show the highest share of initiated payments that never complete?
   
a)By payment method

<img width="800" height="392" alt="image" src="https://github.com/user-attachments/assets/b4f73794-1a88-4c76-9915-d768056ceaba" />

b)By merchant

<img width="800" height="495" alt="image" src="https://github.com/user-attachments/assets/7a742a1d-b230-4b0d-b1e1-fd47cfc45501" />

c)By both

<img width="800" height="794" alt="image" src="https://github.com/user-attachments/assets/405cc34f-748c-42be-ab2a-d162fbcf8f65" />





## 👩‍💻 Contributors

| Member               | Contribution                            |
| -------------------- | --------------------------------------- |
| **Anup Kumar**       | Tooling & Data Architecture             |
| **Bekarys Toleshov** | Data Model & Data Dictionary            |
| **Hardi Teder**      | Business Brief & Dataset Documentation  |
