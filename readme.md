<a name="readme-top"></a>

<div align="center">
  <h1 align="center">Azure Databricks Retail Data Engineering Pipeline</h1>

  <p align="center">
    A production-grade Medallion Architecture (Bronze, Silver, Gold) handling incremental data loading, Slowly Changing Dimensions (SCD Type 1 & 2), and Delta Live Tables (DLT).
    <br />
  </p>
</div>

<details>
  <summary>Table of Contents</summary>
  <ol>
    <li><a href="#about-the-project">About The Project</a></li>
    <li><a href="#architecture--data-flow">Architecture & Data Flow</a></li>
    <li><a href="#technical-implementation">Technical Implementation</a></li>
    <li><a href="#getting-started">Getting Started</a></li>
    <li><a href="#repository-structure">Repository Structure</a></li>
    <li><a href="#future-enhancements">Future Enhancements</a></li>
  </ol>
</details>

## About The Project

This repository contains an end-to-end data engineering solution built natively in Azure Databricks. Processing a retail dataset (Orders, Customers, Products, and Regions), the pipeline ingests raw Parquet files from Azure Data Lake Storage (ADLS) Gen2 and transforms them into a fully optimized Star Schema ready for BI consumption. 

The project goes beyond basic ETL scripts by implementing enterprise-grade features: idempotent streaming, Unity Catalog governance, Object-Oriented Programming (OOP) in PySpark, and declarative pipelines using Delta Live Tables (DLT).

### Built With

* **Azure Databricks** (Compute, Workflows, Serverless SQL Warehouses)
* **Azure Data Lake Storage Gen2** (Hierarchical Storage)
* **PySpark & Spark Structured Streaming**
* **Unity Catalog** (Data Governance & Function Management)

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## Architecture & Data Flow

![High Level Architecture](images/hf_20260323_091022_611c91af-2560-4d3a-8dae-bd5bf7cdb87b.jpg)
*Pipeline conceptual flow from Azure ADLS Gen2 through Databricks processing layers into Power BI.*

The pipeline strictly adheres to the Medallion Architecture design pattern, fully managed via Unity Catalog:

![Unity Catalog](images/Screenshot%202026-03-23%20at%203.00.33%20PM.jpg)
*Unity Catalog configuration showing isolated schemas for Bronze, Silver, and Gold data layers within the `databricks_cata` catalog.*

1. **Source / ADLS Gen2:** Raw retail data (Parquet format) is uploaded incrementally into an ADLS Gen2 container.
2. **Bronze Layer (Raw Ingestion):** Databricks Auto Loader ingests new files idempotently, handling schema evolution and saving data in its rawest state.
3. **Silver Layer (Cleansed & Enriched):** Data is cleaned, data types are cast, missing values are handled, and complex transformations are executed.
4. **Gold Layer (Star Schema):** Data is modeled into Fact and Dimension tables featuring Slowly Changing Dimensions (SCD).
5. **Serving:** Databricks Serverless SQL Warehouse connects the optimized Gold tables to Power BI.

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## Technical Implementation

### 🥉 Bronze Layer
* **Dynamic Auto Loader:** Utilizes Spark Structured Streaming (`cloudFiles`) to incrementally read Parquet files.
* **Idempotency & Checkpointing:** Uses RocksDB state stores to guarantee exactly-once processing. 
* **Workflow Orchestration:** Managed via a parameterized Databricks workflow DAG that iterates over source folders dynamically.

![Bronze Workflow DAG](images/Screenshot%202026-03-23%20at%202.54.45%20PM.jpg)
*Databricks Workflow executing the dynamic Bronze Auto Loader tasks sequentially from Parameters to Iteration.*

### 🥈 Silver Layer
* **Data Cleansing & Enrichment:** Drops `_rescued_data` columns, standardizes timestamps, and applies custom logic.
* **Modular Codebase:** Separate processing notebooks for `Customers`, `Orders`, `Products`, and `Regions` to ensure code maintainability.

### 🥇 Gold Layer
* **SCD Type 1 (`Dim_Customers`):** PySpark Upsert (`MERGE INTO`) logic successfully handles slowly changing customer profiles, automating surrogate key generation and timestamp updates based on the `init_load_flag`.
* **SCD Type 2 (`Dim_Products`):** Delta Live Tables (DLT) declarative pipelines automate history tracking (managing `__START_AT` and `__END_AT` dimensions) while enforcing data quality via DLT Expectations.
* **Fact Table (`Fact_Orders`):** Aggregates business metrics by joining dimension surrogate keys, dropping natural raw keys to optimize warehouse storage.

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## Getting Started

### Prerequisites
* An active Azure Subscription.
* Azure Databricks Workspace (Premium tier recommended for full Unity Catalog features).
* An ADLS Gen2 Account with Hierarchical Namespaces enabled.

### Setup Instructions
1. **Infrastructure Configuration:**
   * Create an **Access Connector for Azure Databricks**.
   * Assign the `Storage Blob Data Contributor` IAM role to the connector for your ADLS account.
2. **Unity Catalog Initialization:**
   * Configure the Unity Catalog Metastore and map it to your ADLS `metastore` container.
   * Create External Locations for the `source`, `bronze`, `silver`, and `gold` storage containers.
3. **Workspace Import:**
   * Clone this repository and import the notebooks into your Databricks workspace.

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## Repository Structure

Based on the Databricks Workspace setup, the codebase is structured as follows:

![Databricks Workspace](images/Screenshot%202026-03-23%20at%202.51.09%20PM.png)
*Databricks workspace organization showcasing modular notebook architecture.*

```text
├── images/
│   ├── hf_20260323_091022_611c91af-2560-4d3a-8dae-bd5bf7cdb87b.jpg
│   ├── Screenshot 2026-03-23 at 2.51.09 PM.png
│   ├── Screenshot 2026-03-23 at 2.54.45 PM.jpg
│   └── Screenshot 2026-03-23 at 3.00.33 PM.jpg
├── src/
│   ├── bronze/
│   │   ├── parameters.py
│   │   └── Bronze Layer.py
│   ├── silver/
│   │   ├── Silver Layer Customers.py
│   │   ├── Silver Layer Orders.py
│   │   ├── Silver Layer Products.py
│   │   └── Silver Regions.py
│   └── gold/
│       ├── Gold Customers.py
│       ├── Gold Products DLT.py
│       └── Fact Orders.py
└── README.md
