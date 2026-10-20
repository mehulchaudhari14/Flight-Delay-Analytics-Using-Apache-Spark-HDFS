# ✈️ Flight Delay Analytics Using Apache Spark & HDFS

> **Big Data Analytics (BDA) Lab Project**  
> **Course:** Big Data Analytics (7th Semester)  
> **Architecture:** HDFS (Distributed Storage) + Apache Spark (PySpark & Spark SQL) + Streamlit & Plotly (Interactive Dashboard)  
> **Dataset:** Bureau of Transportation Statistics (BTS) Flight On-Time Reporting (539,747 records, 25 columns)

---

## 📋 Table of Contents
1. [Project Overview](#-project-overview)
2. [Technology Stack](#-technology-stack)
3. [System Architecture](#-system-architecture)
4. [Dataset Specifications](#-dataset-specifications)
5. [Repository Structure](#-repository-structure)
6. [Prerequisites & Environment Setup](#-prerequisites--environment-setup)
7. [Step-by-Step Execution Guide](#-step-by-step-execution-guide)
8. [Faculty Demonstration & Verification Guide](#-faculty-demonstration--verification-guide)
9. [Spark SQL Analytical Queries & Results](#-spark-sql-analytical-queries--results)
10. [Troubleshooting & FAQ](#-troubleshooting--faq)

---

## 🎯 Project Overview

This project implements an end-to-end Big Data Analytics pipeline analyzing over **539,000 real-world US commercial flights** to uncover delay patterns, bottlenecks, and root causes without reliance on machine learning models, message queues, or relational/NoSQL databases.

### Key Objectives:
- **Distributed Big Data Storage:** Store raw and cleaned datasets reliably across HDFS NameNode and DataNodes.
- **Scalable In-Memory Processing:** Leverage Apache Spark 4.x with PySpark and Spark SQL for data validation, cleaning, and multi-dimensional aggregations.
- **Root Cause Decomposition:** Analyze delay factors across FAA standard categories (Carrier, Weather, NAS, Security, Late Aircraft).
- **Interactive Visual Analytics:** Provide faculty and stakeholders with a dynamic dashboard featuring interactive KPI cards, Plotly charts, filters, and drill-down tables.

---

## 🛠️ Technology Stack

| Layer | Technology | Version / Image | Purpose |
|---|---|---|---|
| **OS Environment** | Ubuntu on WSL2 / Linux | Ubuntu 22.04 LTS | Host environment for containerized orchestration |
| **Containerization** | Docker & Docker Compose | Compose v2.x | Multi-container orchestration (5 isolated services) |
| **Distributed Storage** | Apache Hadoop HDFS | `bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8`<br>`bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8` | Persistent distributed file system for raw CSV & Parquet data |
| **Big Data Engine** | Apache Spark | `apache/spark:latest` (Spark 4.2.0) | In-memory distributed data processing & Spark SQL querying |
| **Query Engine** | Spark SQL / PySpark | Python 3.10 / PySpark | Declarative distributed SQL queries and dataframe transformations |
| **Analytics Dashboard**| Streamlit & Plotly | Streamlit 1.39+, Plotly 5.24+ | Modern interactive web visualization dashboard |

*Strictly avoids Machine Learning, Kafka, Hive metastore, or external databases.*

---

## 🏛️ System Architecture

```mermaid
flowchart TD
    subgraph Ingestion ["1. Data Ingestion & Storage"]
        RawCSV["Raw BTS Flight Data<br/>(T_ONTIME_REPORTING.csv - 61MB)"]
        HDFS_Raw["HDFS: /flight_data/raw/<br/>(Replication=1, Block Distribution)"]
        RawCSV -->|upload_to_hdfs.sh| HDFS_Raw
    end

    subgraph Processing ["2. Apache Spark Processing Engine"]
        HDFS_Raw -->|ingest_flight_data.py| SparkIngest["Spark Ingestion<br/>Schema Validation & Counts"]
        SparkIngest -->|preprocess.py| SparkClean["Spark Data Cleaning<br/>Deduplication, Imputation & Feature Eng"]
        SparkClean -->|Parquet Snappy Write| HDFS_Cleaned["HDFS: /flight_data/cleaned/<br/>(Optimized Columnar Parquet)"]
        HDFS_Cleaned -->|flight_analytics.py| SparkSQL["Spark SQL Analytics Engine<br/>7 Analytical Transformations"]
    end

    subgraph Storage ["3. Analytical Results Storage"]
        SparkSQL -->|Distributed CSV Export| HDFS_Analytics["HDFS: /flight_data/analytics/<br/>kpi_metrics, airline_delays, routes..."]
        HDFS_Analytics -->|export_from_hdfs.sh<br/>(hdfs dfs -getmerge)| LocalSync["Shared Volume: /data/analytics/<br/>Consolidated Summary Datasets"]
    end

    subgraph Presentation ["4. Visual Analytics Layer"]
        LocalSync -->|Instant Cached Read| Streamlit["Streamlit Interactive Dashboard<br/>Port 8501"]
        Streamlit --> PlotlyCharts["7 Interactive Analytic Tabs<br/>KPIs, Airlines, Trends, Airports, Routes, Causes"]
    end

    classDef storage fill:#1e293b,stroke:#60a5fa,stroke-width:2px,color:#fff;
    classDef spark fill:#7c2d12,stroke:#f97316,stroke-width:2px,color:#fff;
    classDef dash fill:#064e3b,stroke:#10b981,stroke-width:2px,color:#fff;

    class HDFS_Raw,HDFS_Cleaned,HDFS_Analytics storage;
    class SparkIngest,SparkClean,SparkSQL spark;
    class Streamlit,PlotlyCharts dash;
```

---

## 📊 Dataset Specifications

- **Source:** United States Department of Transportation — Bureau of Transportation Statistics (BTS)
- **Reporting Period:** Monthly Airline On-Time Performance Data
- **Total Records:** `539,747` flights
- **Cleaned Valid Records:** `539,746` flights (1 duplicate record eliminated)
- **Key Schema Attributes:**
  - `FL_DATE`: Flight Date
  - `OP_UNIQUE_CARRIER`: Carrier Airline Code (WN, AA, DL, UA, OO, etc.)
  - `ORIGIN`, `DEST`: Origin and Destination 3-letter IATA Airport Codes
  - `DEP_TIME`, `DEP_DELAY`: Scheduled vs Actual Departure & Departure Delay (minutes)
  - `ARR_TIME`, `ARR_DELAY`: Scheduled vs Actual Arrival & Arrival Delay (minutes)
  - `CANCELLED`, `CANCELLATION_CODE`: Flight cancellation flag (0/1) and cause code (A=Carrier, B=Weather, C=NAS, D=Security)
  - `DIVERTED`: Flight diversion indicator (0/1)
  - `CARRIER_DELAY`, `WEATHER_DELAY`, `NAS_DELAY`, `SECURITY_DELAY`, `LATE_AIRCRAFT_DELAY`: Delay breakdown in minutes

---

## 📁 Repository Structure

```
BDA Lab Project/
├── config/
│   └── spark-defaults.conf       # Spark tuning & defaultFS configuration
├── dashboard/
│   └── app.py                    # Complete Streamlit + Plotly interactive dashboard
├── data/
│   ├── analytics/                # Consolidate CSV exports from HDFS for dashboard
│   ├── cleaned/                  # Local mount for cleaned Parquet files
│   └── raw/
│       └── T_ONTIME_REPORTING.csv # 539,747-row BTS raw flight dataset
├── scripts/
│   ├── upload_to_hdfs.sh         # Creates HDFS folders and uploads raw CSV
│   ├── export_from_hdfs.sh       # Uses hdfs dfs -getmerge to sync analytics to local
│   └── run_pipeline.sh           # End-to-end master automation script
├── spark_jobs/
│   ├── ingest_flight_data.py     # Stage 1: Ingest from HDFS & schema validation
│   ├── preprocess.py             # Stage 2: Data cleaning & Parquet conversion to HDFS
│   └── flight_analytics.py       # Stage 3: Spark SQL 7-query analytical execution
├── docker-compose.yml            # 5-service container architecture
├── Dockerfile                    # Dashboard container definition (Python 3.10)
├── requirements.txt              # Dashboard Python dependencies
└── README.md                     # Comprehensive project documentation
```

---

## ⚙️ Prerequisites & Environment Setup

### 1. Requirements:
- Windows 10/11 with **WSL2 (Ubuntu 20.04 or 22.04)**
- **Docker Desktop** installed and configured to integrate with WSL2
- At least 8 GB RAM and 15 GB free disk space

### 2. Start Docker Services:
Run from WSL terminal inside the project directory:

```bash
cd "/mnt/d/Final Year 7th Sem/Big Data Analytics/BDA Lab Project"

# Start the complete 5-container cluster
docker compose up -d
```

Verify that all 5 containers are running and healthy:
```bash
docker ps
```
Containers:
1. `flight_namenode` (Ports: `9870`, `9000`)
2. `flight_datanode` (Port: `9864`)
3. `spark-master` (Ports: `8085`, `7077`)
4. `spark-worker` (Port: `8086`)
5. `flight_dashboard` (Port: `8501`)

---

## 🚀 Step-by-Step Execution Guide

You can run the entire pipeline with a single master script, or step-by-step:

### Automated Master Execution:
```bash
bash scripts/run_pipeline.sh
```

---

### Manual Step-by-Step Execution:

#### Step 1: Upload Dataset to HDFS
Uploads `data/raw/T_ONTIME_REPORTING.csv` to `hdfs://namenode:9000/flight_data/raw/`:
```bash
bash scripts/upload_to_hdfs.sh
```

#### Step 2: Spark Data Ingestion & Validation
Reads raw CSV directly from HDFS, verifies schema, column data types, and counts:
```bash
docker exec spark-master /opt/spark/bin/spark-submit \
    --master local[*] \
    --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
    /opt/spark_jobs/ingest_flight_data.py
```

#### Step 3: Spark Data Cleaning & Parquet Transformation
Removes duplicates, handles null values, derives FAA delay indicators, and saves snappy-compressed columnar Parquet files into `hdfs://namenode:9000/flight_data/cleaned/`:
```bash
docker exec spark-master /opt/spark/bin/spark-submit \
    --master local[*] \
    --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
    /opt/spark_jobs/preprocess.py
```

#### Step 4: Spark SQL Big Data Analytics
Executes 7 Spark SQL queries on cleaned Parquet data, outputting results into `hdfs://namenode:9000/flight_data/analytics/`:
```bash
docker exec spark-master /opt/spark/bin/spark-submit \
    --master local[*] \
    --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
    /opt/spark_jobs/flight_analytics.py
```

#### Step 5: Sync Analytics from HDFS to Dashboard Storage
Uses `hdfs dfs -getmerge` to export consolidated summary metrics:
```bash
bash scripts/export_from_hdfs.sh
```

---

## 🎓 Faculty Demonstration & Verification Guide

Follow these commands and URLs to present the Big Data project to your examiner or faculty:

### 1. Web User Interfaces
Open the following links in any web browser:
- 🌐 **HDFS NameNode Web UI:** [http://localhost:9870](http://localhost:9870)
  - Navigate to **Utilities** → **Browse the file system**
  - Show the `/flight_data` directory structure:
    - `/flight_data/raw/T_ONTIME_REPORTING.csv` (61 MB)
    - `/flight_data/cleaned/flight_data_cleaned.parquet/` (10 Parquet partitions)
    - `/flight_data/analytics/` (7 Spark aggregation directories)
- ⚡ **Apache Spark Master Web UI:** [http://localhost:8085](http://localhost:8085)
  - Shows connected Worker node (`spark-worker`), allocated CPU cores, and completed applications.
- 📊 **Streamlit Analytics Dashboard:** [http://localhost:8501](http://localhost:8501)
  - Demonstrates interactive KPI cards, delay distribution charts, airline rankings, and filter controls.

### 2. Command-Line HDFS Verification Commands
Run these commands to prove HDFS persistence and distributed storage:

```bash
# List all files in HDFS raw data storage
docker exec flight_namenode hdfs dfs -ls -h /flight_data/raw

# List HDFS Parquet partitions created by Spark
docker exec flight_namenode hdfs dfs -ls -h /flight_data/cleaned/flight_data_cleaned.parquet

# List all 7 Spark SQL analytics output directories in HDFS
docker exec flight_namenode hdfs dfs -ls -h /flight_data/analytics

# Inspect top 10 rows of Airline Delays stored inside HDFS
docker exec flight_namenode hdfs dfs -cat /flight_data/analytics/airline_delays/part-*.csv | head -n 11
```

---

## 📈 Spark SQL Analytical Queries & Results

The project computes seven multi-dimensional analytics directly using Spark SQL:

### 1. Big Data High-Level KPIs
- **Total Flights Processed:** `539,746`
- **Completed Flights:** `523,435` (96.98%)
- **Delayed Flights (FAA Standard >15 mins):** `94,824` (**18.12%**)
- **Average Departure Delay:** `+10.28 minutes`
- **Average Arrival Delay:** `+3.75 minutes`
- **Cancelled Flights:** `16,311` (**3.02%**)
- **Diverted Flights:** `1,166` (**0.22%**)
- **Total Flight Distance Traveled:** `455,117,995 miles`

### 2. Airline Performance Comparison
Ranking top commercial carriers by flight volume and delay rate:
- **Southwest Airlines (WN):** 118,485 flights | 18.4% delay rate | Avg Arr Delay: +4.9m
- **American Airlines (AA):** 89,240 flights | 19.3% delay rate | Avg Arr Delay: +6.1m
- **Delta Air Lines (DL):** 84,520 flights | **15.1% delay rate (Top Tier)** | Avg Arr Delay: +3.8m
- **United Airlines (UA):** 68,910 flights | 19.4% delay rate | Avg Arr Delay: +5.8m
- **Frontier Airlines (F9):** 15,820 flights | **24.1% delay rate (Highest)** | Avg Arr Delay: +9.9m

### 3. Temporal Delay Trends (Day of Week & Monthly)
- Peak delay frequencies occur on **Thursdays and Fridays** due to combined business and weekend travel congestion.
- **Tuesdays and Wednesdays** demonstrate the lowest average delays (~12.8% delay frequency).

### 4. Delay Root Cause Decomposition (FAA Categories)
- **Carrier Delay (Airline Operations & Crew):** **38.2%** of total delay minutes
- **Late Aircraft Delay (Downstream Ripple Effects):** **33.1%** of total delay minutes
- **NAS Delay (National Airspace System / ATC):** **21.1%** of total delay minutes
- **Extreme Weather Delay:** **6.9%** of total delay minutes
- **Security Screening Delay:** **0.7%** of total delay minutes

### 5. Cancellation Reason Breakdown
- **Carrier Cancellations (Code A):** 46.2% of all cancellations
- **Weather Cancellations (Code B):** 38.9% of all cancellations
- **Air Traffic / NAS Cancellations (Code C):** 14.8% of all cancellations
- **Security Cancellations (Code D):** 0.1% of all cancellations

---

## 🔧 Troubleshooting & FAQ

### Q: Port 8080 conflict during startup?
> In `docker-compose.yml`, the Spark Master Web UI is explicitly remapped to host port **8085** (`8085:8080`) to avoid conflicts with other local services or Jenkins/Tomcat.

### Q: How does the Streamlit Dashboard access Spark analytics?
> Spark saves all aggregations into HDFS partitions under `/flight_data/analytics/`. The `scripts/export_from_hdfs.sh` script executes `hdfs dfs -getmerge` inside the NameNode container to merge and export clean CSVs into the shared Docker volume (`./data/analytics`), allowing the Streamlit container to load the metrics with sub-second latency.

### Q: How do I cleanly shut down or reset the cluster?
```bash
# Stop containers without deleting HDFS volumes
docker compose stop

# Restart stopped containers
docker compose start

# Full reset (removes containers and HDFS volumes)
docker compose down -v
```

---

## 🏆 Project Accomplishments
- ✅ Built and orchestrated a 5-node distributed Big Data architecture using Docker Compose.
- ✅ Uploaded and stored 539,747 flight records in HDFS with full NameNode Web UI visibility.
- ✅ Implemented PySpark schema validation and Parquet columnar compression on HDFS.
- ✅ Executed 7 Spark SQL queries answering real-world aviation performance questions.
- ✅ Created an interactive Streamlit + Plotly visual dashboard accessible at `http://localhost:8501`.
- ✅ Followed Big Data engineering best practices without using ML or external database shortcuts.
