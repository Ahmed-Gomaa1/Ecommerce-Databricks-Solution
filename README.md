# Ecommerce-Databricks-Solution
# Scalable eCommerce ETL Pipeline on Databricks

This repository contains the full solution for the Data Engineer Challenge, demonstrating the design and implementation of a scalable, production-ready ETL pipeline using Databricks. The solution ingests raw eCommerce data, transforms it using the Medallion Architecture, and creates business-ready tables for analytics while also providing a path for real-time streaming.

## Table of Contents
1. [Project Overview](#1-project-overview)
2. [Architecture](#2-architecture)
    - [The Medallion Model](#the-medallion-model)
    - [Architecture Diagram](#architecture-diagram)
    - [Technology Stack](#technology-stack)
3. [Data Model](#3-data-model)
    - [Silver Layer Schema](#silver-layer-schema)
    - [Gold Layer Schemas](#gold-layer-schemas)
4. [Implementation Details](#4-implementation-details)
    - [Project Structure](#project-structure)
    - [Batch Pipeline (Daily ETL)](#batch-pipeline-daily-etl)
    - [Streaming Pipeline (Real-Time Ingestion)](#streaming-pipeline-real-time-ingestion)
5. [How to Run the Solution](#5-how-to-run-the-solution)
    - [Prerequisites](#prerequisites)
    - [Step-by-Step Instructions](#step-by-step-instructions)
6. [Productionalization & Next Steps](#6-productionalization--next-steps)
    - [Orchestration](#orchestration)
    - [Data Quality & Monitoring](#data-quality--monitoring)
    - [Future Enhancements](#future-enhancements)

---

## 1. Project Overview

### The Business Problem
An eCommerce company's valuable user behavior and item data was stored in raw, separate CSV files. This siloed and unprocessed format created significant challenges for data analysts and scientists, hindering their ability to derive insights and build machine learning models. The key challenge was to design a modern data platform to provide unified, clean, and accessible data for both historical analysis and real-time applications.

### The Solution
This project implements a robust ETL pipeline on the Databricks platform. It follows the Medallion Architecture to progressively refine data from its raw state (Bronze) to an enriched source of truth (Silver) and finally to business-ready aggregated tables (Gold). The entire solution is modular, version-controlled via Git, and includes both a daily batch pipeline and a real-time streaming pipeline.

---

## 2. Architecture

### The Medallion Model
The Medallion Architecture is a best practice for structuring data in a data lakehouse. It ensures data quality, reliability, and allows for reprocessing.

*   **Bronze Layer:** Contains the raw, unaltered data ingested directly from the source CSV files. This layer serves as a historical archive. The split `item_properties` files are combined here.
*   **Silver Layer:** Provides a validated, enriched, and queryable source of truth. Data from all sources is cleaned and joined. The critical challenge of handling time-varying item properties (e.g., price changes) is solved in this layer using a **point-in-time join**.
*   **Gold Layer:** Contains highly refined, aggregated tables tailored for specific business use cases like sales reporting, funnel analysis, and customer segmentation.

### Architecture Diagram
![Architecture Flow Diagram](https://i.imgur.com/your-diagram-image-url.png)
*(**Action:** You will need to create this diagram using a tool like draw.io or Lucidchart and upload the image, then replace the URL above)*

**Flow Description:**
1.  **Ingestion:** Raw CSV files land in a Unity Catalog Volume.
2.  **Batch & Streaming Pipelines:** Both pipelines read from this volume.
3.  **Bronze:** The raw data is ingested into Delta tables (`events_bronze`, `properties_bronze`, etc.).
4.  **Silver:** A complex transformation joins the bronze tables to create the `events_enriched_silver` table.
5.  **Gold:** A series of aggregation jobs run on the silver table to create business-ready tables (e.g., `daily_sales_gold`).
6.  **End Users:** Data scientists, analysts, and BI tools consume the clean data from the Gold and Silver layers.

### Technology Stack
| Tool / Technology | Purpose |
| :--- | :--- |
| **Databricks** | Unified platform for data engineering and data science. |
| **Unity Catalog** | Governed storage for data, models, and files (Volumes). |
| **Databricks Repos**| Git integration for version control and CI/CD. |
| **Apache Spark** | Core distributed processing engine. |
| **PySpark** | Python API used to write the ETL logic. |
| **Delta Lake** | Storage format for all tables, providing reliability (ACID) and performance. |
| **Databricks Workflows**| Job orchestration and scheduling for the batch pipeline. |
| **Spark Structured Streaming** | Engine for the real-time ingestion pipeline, using Auto Loader. |

---

## 3. Data Model
*(**Action:** Create a visual ERD-style diagram for this section and link the image)*

### Silver Layer Schema
**Table: `events_enriched_silver`** - The primary, unified table.
| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| `timestamp` | long | Unix timestamp of the event. |
| `visitorid` | int | Unique visitor identifier. |
| `event` | string | 'view', 'addtocart', or 'transaction'. |
| `itemid` | int | Unique item identifier. |
| `transactionid`| int | Transaction identifier (null for non-transaction events). |
| `item_category_id` | string | The category of the item at the time of the event. |
| `parent_category_id` | string | The parent category of the item. |
| `available` | string | "1" (in stock) or "0" (out of stock). |
| `price_property` | string | The raw price property (e.g., "n12000.000"). |

### Gold Layer Schemas
**Table: `fact_sales_transactions_gold`**
| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| `transactionid` | int | Unique transaction identifier. |
| `date` | date | The calendar date of the transaction. |
| `price` | decimal(10,2)| The price of the item at the time of purchase. |
| ... | ... | (visitorid, itemid, item_category_id, etc.) |

**Table: `conversion_funnel_gold`**
| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| `itemid` | int | Unique item identifier. |
| `unique_views` | long | Number of unique visitors who viewed the item. |
| `unique_addtocarts` | long | Number of unique visitors who added the item to cart. |
| `unique_purchases` | long | Number of unique visitors who purchased the item. |
| `view_to_cart_rate` | double | Conversion rate from view to cart. |

---

## 4. Implementation Details

### Project Structure
The code is organized into a modular structure within the `etl_pipeline` directory, managed via Databricks Repos.
.
├── etl_pipeline/
│ ├── reader.py # Functions for data extraction (Bronze)
│ ├── transformers.py # Core business logic (Silver & Gold)
│ └── writer.py # Functions for writing Delta tables
├── Main Pipeline.py # Orchestrator notebook for the batch job
└── Streaming Pipeline.py # Orchestrator notebook for the streaming job
code
Code
### Batch Pipeline (Daily ETL)
The `Main Pipeline` notebook orchestrates the daily batch job.
1.  **Extract:** The `reader.read_source_data` function reads all CSV files from the source Volume and combines the partitioned `item_properties` files.
2.  **Transform:** The `transformers.create_silver_layer` function is called to perform the complex point-in-time join and create the unified `events_enriched_silver` table.
3.  **Load:** The `writer.write_delta_table` function saves the bronze and silver tables.
4.  **Aggregate:** The `transformers.create_gold_layers` function is called to generate all the business-ready Gold tables from the Silver table. These are then saved using the writer function.

### Streaming Pipeline (Real-Time Ingestion)
The `Streaming Pipeline` notebook demonstrates how to evolve the architecture for real-time needs.
1.  **Extract:** It uses **Spark Structured Streaming with Auto Loader** (`spark.readStream`) to watch the source Volume directory. It is configured to only process new files matching the pattern `*events*.csv`.
2.  **Transform (Micro-batch):** For each small "micro-batch" of new data, the `.forEachBatch` command is used. Inside this command, we call our existing `create_silver_layer` function, proving the reusability of our code. This function joins the incoming stream of events against a static read of the properties and categories tables.
3.  **Load:** The transformed micro-batch is appended to a separate streaming target table (`events_enriched_silver_stream`). Checkpoints are stored in the UC Volume to ensure data is processed exactly once.

---

## 5. How to Run the Solution

### Prerequisites
1.  A Databricks workspace (Community Edition is sufficient).
2.  A Git repository (e.g., on GitHub) cloned into Databricks Repos.
3.  The source CSV files uploaded to a Unity Catalog Volume (e.g., `/Volumes/workspace/e-commerce_data/csv_files/`).
4.  A running cluster.

### Step-by-Step Instructions
1.  **Clone the Repository:** Add this repository to your Databricks workspace via the Repos UI.
2.  **Configure Paths:** In the `Main Pipeline` and `Streaming Pipeline` notebooks, update the `sys.path.append()` line to point to the correct path of your repo in your workspace.
3.  **Run the Batch Pipeline:**
    *   Open the `Main Pipeline` notebook.
    *   Ensure the widgets at the top point to the correct source volume and target database.
    *   Click "Run All". This will create all the batch tables in the specified database (e.g., `default`).
4.  **Run the Streaming Pipeline:**
    *   Open the `Streaming Pipeline` notebook.
    *   Ensure the widgets are correctly configured, especially the checkpoint path which must point to a location within a UC Volume.
    *   Run the notebook. It will process the existing event files and then stop (due to the `trigger(availableNow=True)` setting).
    *   To test the streaming capability, upload a new CSV file with event data into the source Volume and re-run the notebook. It will only process the new file.

---

## 6. Productionalization & Next Steps

### Orchestration
The `Main Pipeline` notebook is production-ready and can be scheduled using **Databricks Workflows (Jobs)**.
*   **Job Configuration:** A job is created to run the `Main Pipeline` notebook on a cost-effective Job Cluster.
*   **Scheduling:** The job is set to a CRON schedule (e.g., daily at 3:00 AM) to automate the entire ETL process.
*   **Note:** The free Databricks Community Edition has a simplified Jobs feature that allows scheduling on the interactive cluster. In a standard workspace, dedicated Job Clusters would be used for better cost-efficiency and isolation.

### Data Quality & Monitoring
*   **Data Quality Checks:** The `transformers.py` module includes `assert` statements that act as data quality checks (e.g., checking for null keys or invalid values). If a check fails, the pipeline will stop and raise an error.
*   **Monitoring & Alerting:** The Databricks Job is configured with **email alerts** to notify the engineering team upon any job failure, allowing for immediate investigation and resolution.

### Future Enhancements
*   **Full DLT Migration:** For a fully managed streaming solution, the entire pipeline could be migrated to a **Delta Live Tables (DLT)** pipeline. DLT would automate infrastructure management, data quality monitoring, and simplify the code.
*   **Advanced Data Quality:** Implement a more robust data quality framework like **Great Expectations** within the DLT pipeline to define and track a comprehensive suite of expectations.
*   **CI/CD:** Integrate the Databricks Repo with GitHub Actions (or another CI/CD tool) to automate the testing and deployment of pipeline code from development to production environments.

