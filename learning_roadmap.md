# Structured Learning Roadmap: Azure Data Engineer (Job-Ready in 10-12 Weeks)

This roadmap is structured to take you from knowing nothing about Data Engineering to passing technical interviews and landing your first job.

---

## 📅 Phase 1: The Core Foundations (Weeks 1 - 3)
*Goal: Master the two essential languages of data.*

### 1. SQL (The Absolute Priority - 70% of Interviews)
*   **Concepts to Learn**:
    *   DDL/DML (Creating tables, inserting, updating data).
    *   Joins (INNER, LEFT, RIGHT, FULL, CROSS) and Subqueries.
    *   **CTEs (Common Table Expressions)**: Essential for organizing complex queries.
    *   **Window Functions**: `ROW_NUMBER()`, `RANK()`, `DENSE_RANK()`, `LEAD()`, `LAG()`, `SUM() OVER(PARTITION BY...)`.
*   **Resources**: Mode Analytics SQL Tutorial, LeetCode (Easy/Medium SQL questions), Hackerrank.
*   **Your Code Reference**: Study `02_SQL_Basics/sql_basics.sql` in this repository.

### 2. Python (Scripting & Pandas)
*   **Concepts to Learn**:
    *   Variables, Loops (`for`, `while`), Functions, Error Handling (`try-except`).
    *   File I/O (Reading/writing CSV and JSON files).
    *   **Pandas Library**: DataFrames, selecting columns, filtering, grouping, merging data.
*   **Your Code Reference**: Study `01_Python_Basics/python_basics.py` in this repository.

---

## 📅 Phase 2: Distributed Computing & PySpark (Weeks 4 - 5)
*Goal: Understand how to process gigabytes/terabytes of data using Spark.*

### 1. PySpark Foundations
*   **Concepts to Learn**:
    *   What is distributed computing? (Driver node vs. Worker nodes).
    *   PySpark DataFrames (Select, Filter, GroupBy, Join, WithColumn).
    *   Lazy Evaluation and Spark Execution Plans (Actions vs. Transformations).
*   **Your Code Reference**: Study `03_PySpark_Fundamentals/pyspark_basics.py` in this repository.

### 2. Delta Lake Storage
*   **Concepts to Learn**:
    *   What is Delta Lake? (Parquet files + Transaction Log).
    *   ACID properties, Schema Enforcement, Time Travel (reading older versions of data).
    *   The `MERGE INTO` operation (Upserting: updating existing rows and inserting new ones).

---

## 📅 Phase 3: Azure Storage & Databricks Compute (Weeks 6 - 7)
*Goal: Learn how to set up the data warehouse and run Spark in the cloud.*

### 1. Azure Storage (ADLS Gen2)
*   **Concepts to Learn**:
    *   Hierarchical Namespace (folders vs. flat blobs).
    *   Access keys, Shared Access Signatures (SAS), and Service Principals (App Registrations).

### 2. Azure Databricks (ADB)
*   **Concepts to Learn**:
    *   How to create Clusters (Single node vs. Multi-node).
    *   Writing code in Databricks Notebooks.
    *   Connecting Databricks to ADLS Gen2 (Mounting containers).
    *   **Your Code Reference**: Study `05_Medallion_Architecture_Guide/medallion_architecture.md` to see how files are organized in the lakehouse.

---

## 📅 Phase 4: Data Orchestration with Data Factory (Weeks 8 - 9)
*Goal: Learn how to automate your pipelines.*

### 1. Azure Data Factory (ADF)
*   **Concepts to Learn**:
    *   **Linked Services** (Credentials/connections to Databricks/Storage).
    *   **Datasets** (Pointing to specific files/folders).
    *   **Activities**: Copy Activity, Get Metadata, Web, ForEach, Databricks Notebook execution.
    *   **Triggers**: Scheduled (e.g., every midnight) and Event-based (e.g., run whenever a file is uploaded).
*   **Your Code Reference**: Study `04_Azure_Data_Factory/adf_templates_and_guide.md`.

---

## 📅 Phase 5: Projects & Portfolio Build (Week 10)
*Goal: Connect all the components together.*

*   Implement the two projects in this repository:
    1.  **Project 1**: E-Commerce sales Medallion pipeline.
    2.  **Project 2**: IoT Device Telemetry pipeline.
*   Practice explaining the flow: *"I copy raw files with ADF, process them through Bronze, Silver, Gold using Databricks notebooks, and save them in ADLS Gen2."*

---

## 📅 Phase 6: Job Application & Interview Prep (Weeks 11 - 12)
*Goal: Get noticed by recruiters and pass technical screenings.*

### 1. GitHub Portfolio Setup
*   Your GitHub repository is already set up and pushed: `https://github.com/gatlakiran02/Azure-Data-Engineering`. Use it on your resume!

### 2. Typical Interview Topics
*   **SQL Coding Test**: Practice solving SQL problems live. (Inner vs Left Join, Window functions, CTEs).
*   **Scenario-based Questions**:
    *   *"How do you handle duplicates in your pipeline?"* (Answer: Using `dropDuplicates()` or row number filtering in the Silver zone).
    *   *"How do you handle late-arriving data or schema changes?"* (Answer: Delta Lake schema evolution and merge operations).
    *   *"How do you schedule pipelines?"* (Answer: ADF Triggers).
