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
