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



## 👩‍💻 Contributors

| Member               | Contribution                            |
| -------------------- | --------------------------------------- |
| **Anup Kumar**       | Tooling & Data Architecture             |
| **Bekarys Toleshov** | Data Model & Data Dictionary            |
| **Hardi Teder**      | Business Brief & Dataset Documentation  |
| **Robert Sarnet**    | Demo Queries & Documentation Formatting |
