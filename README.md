# Healthcare Analytics System – ETL Pipeline

This project focuses on building a **Healthcare Analytics System** that leverages an **ETL pipeline** to extract, transform, and load healthcare data for analytical purposes.

The pipeline follows a structured approach:

1. **Extract**: Data is extracted from a local **Amazon DynamoDB** database into **Databricks Notebooks**.
2. **Transform**: Using **PySpark**, the extracted data is transformed into meaningful analytical structures.
3. **Load**: The transformed data is stored in **Amazon Redshift (AWS Data Warehouse)** for efficient querying and reporting.

<br />

## **Project Goals**

The system extracts healthcare treatment data, processes it, and loads it into **7 analytical tables** in Amazon Redshift to enable the following insights:

1. **Rank Providers Based on Total Treatments**
2. **Rank Providers Based on Treatment Success Rate**
3. **Monthly Trends in Treatment Success Rates**
4. **Geographical Distribution of Treatments**
5. **Summary Metrics - Average Treatment Cost**
6. **Summary Metrics - Total Treatments Per City**
7. **Summary Metrics - Provider Success Rate**

<br />

## **Tools & Technologies Used**

### **Data Extraction & Transformation (Bronze & Silver Layers) [ 1st Half ETL Pipeline ]**

- **Azure Databricks Community Edition** → Used for initial data extraction and transformation before transferring to **AWS S3 (Staging Layer).**

### **Data Transfer (Silver → Gold Layer) and Orchestration [ 2nd Half ETL Pipeline ]**

- **Amazon Web Services (AWS Cloud):**
  - **Amazon DynamoDB** – Source database storing **59,452** treatment records (downscaled from an initial 600,000 due to compute and cost limitations).
  - **Amazon S3** – Used as a staging layer to hold processed data before loading it into Redshift.
  - **AWS Lambda** – Automates processing tasks such as file movement and Redshift data transfer.
  - **AWS Step Functions** – Orchestrates different Lambda functions for **pipeline automation.**
  - **AWS CloudWatch** – Monitors and logs execution details of the ETL pipeline.
  - **Amazon Redshift** – The **target Data Warehouse**, where the transformed data is loaded and structured in a **Star Schema** for analytical queries.

<br />

## **Choice of DynamoDB Over DocumentDB**

We considered using **AWS DocumentDB (a MongoDB-compatible service)** but ultimately chose **Amazon DynamoDB** for the following reasons:

- **Learning Opportunity:** I have already worked on MongoDB in a previous project and wanted to gain hands-on experience with DynamoDB.
- **Performance & Cost Efficiency:** DynamoDB provides **on-demand scaling, lower operational overhead, and built-in caching (DAX)** compared to DocumentDB, making it more cost-effective for this project.
- **Serverless Architecture:** Unlike DocumentDB, which requires cluster provisioning, DynamoDB is **fully managed and serverless**, reducing operational complexity.

<br />

## **Data Warehouse Schema – Star Schema Design**

<img src="architecture_diagram\healthcare_analytics_system__star_schema_logical_diagram.png" alt="Data Warehouse Star Schema Logical Diagram">

The Data Warehouse is designed using a **Star Schema**, where:

- **silver.tbl_healthcare_analytics_data** acts as the **source table**.
- **gold.dim tables** are derived from the silver layer, organizing data into dimensions.
- **gold.fact_treatments** is constructed using these dimensions to facilitate analytical queries.

#### **Schema Overview**

- **Silver Table**: `silver.tbl_healthcare_analytics_data`
- **Dimension Tables (Gold Layer)**:
  - `gold.dim_dates`
  - `gold.dim_times`
  - `gold.dim_treatment_types_and_outcome_statuses`
  - `gold.dim_locations`
  - `gold.dim_specialities`
  - `gold.dim_providers`
  - `gold.dim_patients`
  - `gold.dim_diseases`
- **Fact Table (Gold Layer)**:
  - `gold.fact_treatments`
- **ETL Tracking Table**:
  - `gold.etl_tracker`

This schema enables efficient **querying, reporting, and analysis** of healthcare treatment data while maintaining data integrity and performance.

<br />

## **ETL Pipeline Architecture**

<img src="architecture_diagram\Healthcare_Analytics_System__System_Architecture.png" alt="System Architecture">

### **1. Data Extraction & Transformation (Bronze & Silver Layers)**

#### **Bronze Layer (DBFS)**

1. **Extracted Data from DynamoDB** → Written to the **Bronze Layer** in **Azure Databricks (DBFS)**.
2. **Extraction Managed by:** `Extraction_NB.ipynb`

#### **Silver Layer (AWS S3)**

3. **Data Transformation Steps:** (Managed by `Transformation_NB.ipynb`)

   - Convert **DynamoDB items** into **nested JSON**.
   - **Flatten** the JSON structure.
   - **Type-casting** for Redshift compatibility.
   - Write the **transformed data as Parquet files** into **S3 (Silver Layer).**

4. **Incremental vs Full Extraction:**
   - If the **Bronze & Staging layers (S3)** are **empty**, a **full extraction & transfer** is performed.
   - Otherwise, an **incremental extraction & transfer** is done based on the `metadata_modified_at` field.

### **2. Data Load & Orchestration (Silver → Gold Layer)**

<img src="architecture_diagram\Step_Function__State_Machine_Design.png" alt="Step Functions's State Machine [Workflow]">

1. Once transformed data is written to **S3 (Silver Layer),** an **S3 notification** triggers a **Lambda function (`TriggerStepFunctionLambda`)** to invoke the Step Function.
2. The **Step Function** orchestrates the execution of the following AWS Lambda functions in sequence:

   - **`CopyToRedShiftLambda`** → Loads the latest Parquet file from **S3 Silver Layer** into a staging table in **Redshift**.
   - **`TransformToGoldLambda`** → Calls **Redshift stored procedures** to:
     - Populate **dimension (dim) tables** and **fact tables** (Star Schema).
     - Ensure sequential execution of stored procedures.
   - **`ExecuteAnalyticalQueriesLambda`** → Joins data from **dim and fact tables** to populate the **7 analytical tables.**

<br />

## **Conclusion**

This ETL pipeline enables scalable, cost-effective healthcare analytics using **DynamoDB, Databricks, S3, Lambda, Step Functions, and Redshift.**

The **Star Schema** ensures efficient analytical queries for **ranking providers, tracking trends, and summarizing healthcare data.**
