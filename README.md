# ❄️ Snowflake Data Engineering Pipeline

This project demonstrates the implementation of an end-to-end data engineering pipeline using **Snowflake** to process structured and semi-structured datasets into an analytics-ready data warehouse.

The solution ingests multiple **JSON** and **CSV** datasets, stages them in Snowflake, performs data cleansing and transformations through an Operational Data Store (ODS), and loads the processed data into a dimensional warehouse designed for reporting and business analytics.

---

## 🚀 Technologies Used

- Snowflake
- SQL
- JSON
- CSV
- ETL Pipeline
- Data Warehousing
- Dimensional Modeling

---

## 🏗️ Architecture

![Pipeline Architecture](images/architecture.PNG)

The pipeline follows a layered architecture to ensure reliable data processing and efficient analytical reporting.

```
JSON & CSV Files
        │
        ▼
 Snowflake Stage
        │
        ▼
   Staging Layer
        │
        ▼
     ODS Layer
        │
        ▼
 Data Warehouse
        │
        ▼
 Analytics & Reporting
```

---

## 📂 Data Sources

The project combines multiple datasets representing business operations and external environmental data.

### JSON Files

- Business Information
- Customer Reviews
- User Profiles
- Business Check-ins
- COVID Features
- Customer Tips

### CSV Files

- Daily Temperature
- Daily Precipitation

---

## ⚙️ ETL Workflow

### 1️⃣ Extract

- Upload JSON and CSV files into Snowflake Stages.
- Configure file formats for structured data loading.

### 2️⃣ Load to Staging

- Import raw datasets into staging tables using Snowflake's `COPY INTO` command.
- Preserve raw data before transformation.

### 3️⃣ Transform (ODS Layer)

The Operational Data Store performs:

- JSON parsing and flattening
- Data cleansing
- Type casting
- Duplicate handling
- Relationship mapping
- Entity separation

---

## 🏛️ Data Warehouse Layer

After transformation, the cleaned data is organized using dimensional modeling.

### Dimension Tables

- Customer
- Business
- Temperature
- Precipitation

### Fact Table

- Review Facts

The warehouse structure supports efficient joins, aggregations, and analytical reporting.

---

## 📊 Analytics Layer

The final warehouse enables:

- Business performance analysis
- Customer behavior reporting
- Review trend analysis
- SQL-based KPI generation
- Interactive dashboard integration

---

## 📁 Repository Structure

```
Snowflake-Data-pipeline/
│
├── README.md
├── images/
│   └── architecture.PNG
├── SQL Scripts/
│   ├── Stage
│   ├── ODS
│   └── Data Warehouse
└── datasets/
```

---

## ✨ Key Features

- End-to-End ETL Pipeline
- Snowflake Stage Configuration
- JSON & CSV Data Processing
- Operational Data Store (ODS)
- Data Cleansing & Transformation
- Dimensional Data Modeling
- Fact & Dimension Tables
- SQL-Based Analytics

---

## 🎯 Learning Outcomes

This project provided practical experience in:

- Building cloud-based ETL pipelines using Snowflake
- Processing structured and semi-structured datasets
- Designing Operational Data Store (ODS) architectures
- Implementing dimensional data models
- Writing optimized SQL transformations
- Developing analytics-ready data warehouses

---

## 📌 Project Summary

This project demonstrates a complete cloud data engineering workflow, beginning with raw data ingestion and ending with a dimensional warehouse optimized for reporting and analytics. It highlights practical experience with Snowflake staging, ETL development, SQL transformations, and modern data warehouse design.

---

## 📄 License

This repository is intended for educational and portfolio purposes.
