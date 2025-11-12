# Ecommerce Databricks ETL Solution

This repository contains a scalable ETL pipeline built on Databricks to process raw eCommerce data into clean, analytics-ready tables. The solution demonstrates best practices including the Medallion Architecture, modular code, version control, and both batch and streaming capabilities.

## Table of Contents
1. [Architecture & Data Flow](#1-architecture--data-flow)
2. [Data Model](#2-data-model)
3. [Implementation Overview](#3-implementation-overview)
4. [How to Run](#4-how-to-run)
5. [Productionalization](#5-productionalization)

---

## 1. Architecture & Data Flow
![Architecture & Data Flow](Architecture diagram.png)
This project uses the **Medallion Architecture** to progressively refine data across three layers. The entire pipeline is built using a modern Databricks stack including Unity Catalog, Delta Lake, and PySpark.

*   **Bronze Layer:** Raw, unaltered data ingested directly from source CSVs into Delta tables.
*   **Silver Layer:** A single, unified source of truth (`events_enriched_silver`) where all data is joined and cleaned. This layer solves the critical challenge of handling time-varying item properties by performing a **point-in-time join**.
*   **Gold Layer:** Aggregated, business-focused tables optimized for analytics and BI.

**Technology Stack:**
*   **Platform:** Databricks
*   **Storage:** Delta Lake, Unity Catalog Volumes
*   **Processing:** Apache Spark (PySpark)
*   **Orchestration:** Databricks Workflows (Jobs)
*   **Streaming:** Spark Structured Streaming with Auto Loader
*   **Version Control:** Git (via Databricks Repos)

---

## 2. Data Model


### Silver Layer Schema
**Table: `events_enriched_silver`**
| Column | Type | Description |
|---|---|---|
| `timestamp` | long | Unix timestamp of the event. |
| `visitorid` | int | Unique visitor ID. |
| `event` | string | 'view', 'addtocart', or 'transaction'. |
| `itemid` | int | Unique item ID. |
| `transactionid`| int | Unique transaction ID (if applicable). |
| `item_category_id`| string | Category ID of the item. |
| `parent_category_id`| string | Parent category ID. |
| `available` | string | Item availability (1 or 0). |
| `price_property` | string | Raw price property (e.g., "n12000.000"). |

### Gold Layer Schemas
**Table: `fact_sales_transactions_gold`**
| Column | Type | Description |
|---|---|---|
| `transactionid` | int | Transaction identifier. |
| `date` | date | Date of the transaction. |
| `visitorid` | int | ID of the purchasing visitor. |
| `itemid` | int | ID of the item purchased. |
| `price` | decimal(10,2)| Price of the item at time of purchase. |
| `item_category_id` | string | Category ID of the item. |

**Table: `daily_sales_by_category_gold`**
| Column | Type | Description |
|---|---|---|
| `date` | date | Date of aggregation. |
| `item_category_id` | string | Category identifier. |
| `number_of_transactions` | long | Total count of transactions for the category on that day. |

**Table: `customer_segments_gold`**
| Column | Type | Description |
|---|---|---|
| `visitorid` | int | Unique visitor ID. |
| `total_spend` | decimal | Total amount spent by the customer. |
| `number_of_transactions` | long | Total number of transactions made. |
| `segment` | string | Customer value segment (e.g., "Top 500 Spenders"). |

**Table: `conversion_funnel_gold`**
| Column | Type | Description |
|---|---|---|
| `itemid` | int | Unique item identifier. |
| `unique_views` | long | Count of unique visitors who viewed the item. |
| `unique_addtocarts`| long | Count of unique visitors who added to cart. |
| `unique_purchases`| long | Count of unique visitors who purchased. |
| `view_to_cart_rate` | double | Conversion rate from view to add-to-cart. |
| `cart_to_purchase_rate` | double | Conversion rate from add-to-cart to purchase. |

---

## 3. Implementation Overview

The codebase is modular and organized for reusability.

*   **`etl_pipeline/` directory:** Contains the core Python modules.
    *   `reader.py`: Handles data ingestion.
    *   `transformers.py`: Contains all transformation logic for Silver and Gold layers.
    *   `writer.py`: Handles writing data to Delta tables.
*   **`Main Pipeline.py` notebook:** The orchestrator for the batch ETL job. It is parameterized using widgets for flexibility.
*   **`Streaming Pipeline.py` notebook:** An orchestrator for the streaming pipeline that reuses the same transformation logic to process data incrementally.

---

## 4. How to Run

### Prerequisites
1.  A Databricks workspace with a running cluster.
2.  This repository cloned into Databricks Repos.
3.  Source CSV files uploaded to a Unity Catalog Volume.

---

## 5. Productionalization

*   **Orchestration:** The `Main Pipeline` notebook is scheduled to run daily using **Databricks Jobs (Workflows)**. The job is configured to use an ephemeral job cluster for cost optimization and passes production parameters via widgets.
*   **Data Quality:** Automated data quality checks are embedded in the `transformers.py` module. An invalid condition (e.g., a transaction with no ID) will raise an `AssertionError` and fail the pipeline, preventing bad data from being loaded.
*   **Monitoring:** The Databricks Job is configured with **email alerts on failure**, ensuring the data engineering team is notified immediately of any issues.
*   **Future Work:**
    *   **Delta Live Tables (DLT):** Migrate the streaming pipeline to DLT for a fully managed, declarative, and more robust real-time solution.
    *   **CI/CD:** Implement a CI/CD workflow using GitHub Actions to automate testing and deployment of the pipeline code.
