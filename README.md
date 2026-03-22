# 🏦 Banking Analytics & System Monitoring (SQL + Python + Power BI)

## 📌 Project Overview
In real-world banking environments, data privacy restrictions limit the use of production data for analytics and learning.

To overcome this, I built a **simulated core banking system** with 100K+ synthetic records, replicating transaction flows, ATM operations, and system replication behavior.

This project demonstrates **end-to-end data analytics**, from database design and data generation to SQL analysis and dashboard visualization.

---

## 🏗️ Architecture

Python → SQL Database → SQL Views → Power BI Dashboard

---

## 🗄️ Database Design

The system consists of multiple relational tables:

- **core_banking_transactions** → transaction details across channels  
- **atm_logs** → ATM activity and usage  
- **interest_logs** → interest-related entries  
- **replication_metrics** → system replication lag across environments  

---

## ⚙️ Data Generation

- Generated **100K+ records using Python**
- Simulated:
  - Transaction distribution (ATM, Online, Branch)
  - Peak-hour transaction spikes
  - Failure scenarios
  - Replication lag across PR, DR, NR environments

---

## 📊 SQL Analysis

Performed advanced SQL analysis using:

- CTEs (Common Table Expressions)
- Window Functions
- Aggregations
- Views for reporting

### Key Analysis:
- Transaction trends by hour and channel  
- Success vs failure rate  
- Top accounts by transaction value  
- Replication lag monitoring  

---

## 📈 Dashboards (Power BI)

Built interactive dashboards to monitor:

### 1. Core Banking Transactions
- Total transactions (100K+)
- Channel distribution
- Transaction status breakdown
- Hourly transaction trends

### 2. System Performance & Replication Monitoring
- Replication lag across environments (PR, DR, NR)
- Transaction volume vs replication delay
- Location-based performance metrics

---

## 🔍 Key Insights

- Peak transaction volume reached **~20K/hour**
- Online channel contributes highest transaction share (~40%)
- Transaction failure rate ~5%
- Replication lag peaked at **43 seconds**, indicating potential system bottlenecks

---

## 🛠️ Tools & Technologies

- SQL (Advanced Queries, Views)
- Python (Data Generation - Pandas, NumPy)
- Power BI (Dashboarding, DAX)
- Excel (Validation)

---

## 📸 Dashboard Preview

### Core Banking Dashboard
![Dashboard](images/dashboard1.png)

### System Monitoring Dashboard
![Dashboard](images/dashboard2.png)

---

## 🚀 How to Run

1. Run `schema.sql` to create tables  
2. Execute `data_generation.py` to populate data  
3. Run SQL queries/views  
4. Open Power BI file (`.pbix`)  

---

## 💡 Key Learnings

- Designing scalable relational databases  
- Simulating real-world enterprise data  
- Performing performance monitoring using SQL  
- Building business-focused dashboards  

---

## 📬 Connect With Me

- LinkedIn: (add your link)
- GitHub: (your profile link)
