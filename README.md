# 🎬 Real-Time Movie Rating Analytics Pipeline
### Scalable Big Data Streaming & Distributed Analytics with Hadoop MapReduce, Apache Hive, MongoDB & Streamlit

[![Hadoop](https://img.shields.io/badge/Apache%20Hadoop-3.3.6-F09A37?logo=apachehadoop&logoColor=white)](https://hadoop.apache.org/)
[![Hive](https://img.shields.io/badge/Apache%20Hive-3.1.3-FDEE21?logo=apachehive&logoColor=black)](https://hive.apache.org/)
[![MongoDB](https://img.shields.io/badge/MongoDB-8.0-47A248?logo=mongodb&logoColor=white)](https://www.mongodb.com/)
[![Python](https://img.shields.io/badge/Python-3.10-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.37+-FF4B4B?logo=streamlit&logoColor=white)](https://streamlit.io/)
[![Platform](https://img.shields.io/badge/Platform-WSL2%20%7C%20Ubuntu%2022.04-E95420?logo=ubuntu&logoColor=white)](https://ubuntu.com/)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

---

## 📌 Executive Overview

Traditional MapReduce academic demonstrations rely on static CSV files processed in a single batch. This project elevates Big Data engineering by creating an **end-to-end real-time analytics pipeline**:

1. **Simulated Live Stream:** MovieLens ratings arrive dynamically into **MongoDB**.
2. **Checkpointed Incremental Ingestion:** A lightweight loader extracts new ratings (`ts <= now - 5s`) and syncs them into **HDFS** as timestamped TSV chunks with zero duplicates.
3. **Distributed MapReduce on YARN:** A custom Python MapReduce streaming job computes rating aggregates (`count`, `total`, `average`) per movie across distributed worker nodes.
4. **Analytical SQL Engine (Apache Hive):** External Hive tables mapped over HDFS folders run 7 multi-dimensional analytical queries (Top-N movies, genre popularity, arrival trends, rating distribution).
5. **Cross-Validation:** Hive analytical calculations automatically cross-verify MapReduce outputs (Q2 vs. Q3) to ensure 100% calculation consistency.
6. **Live Interactive Dashboard:** A **Streamlit** dashboard displays real-time ingestion throughput, live MongoDB metrics, and analytics charts.

---

## 🏗️ System Architecture

```
                    ┌────────────────────────────────────────┐
                    │ MovieLens ml-latest-small Dataset      │
                    │ (100,836 ratings · 9,742 movies)       │
                    └───────────────────┬────────────────────┘
                                        │ (Simulated Live Streaming)
                                        ▼
                             [ producer.py ]
                                        │
                                        ▼
                         ┌─────────────────────────────┐
                         │   MongoDB (movieapp.ratings)│
                         └──────────────┬──────────────┘
                                        │
                           [ mongo_to_hdfs.py ]
                           (Incremental Checkpoint)
                                        │
                                        ▼
                         ┌─────────────────────────────┐
                         │   HDFS (/bda/movies/ratings)│
                         └──────────────┬──────────────┘
                                        │
                   ┌────────────────────┴────────────────────┐
                   ▼                                         ▼
      ┌─────────────────────────┐               ┌─────────────────────────┐
      │   Hadoop Streaming      │               │       Apache Hive       │
      │       MapReduce         │               │     External Tables     │
      │  (mapper.py/reducer.py) │               │   (movie_analysis.hql)  │
      └────────────┬────────────┘               └────────────┬────────────┘
                   │                                         │
                   ▼                                         ▼
      ┌─────────────────────────┐               ┌─────────────────────────┐
      │ MongoDB (movie_stats)   │               │ Results (results/*.txt) │
      └────────────┬────────────┘               └────────────┬────────────┘
                   │                                         │
                   └────────────────────┬────────────────────┘
                                        │
                                        ▼
                         ┌─────────────────────────────┐
                         │   Streamlit Web Dashboard   │
                         │   (http://localhost:8501)   │
                         └─────────────────────────────┘
```

![Architecture Diagram](docs/architecture.png)

---

## 🚀 Key Features & Engineering Highlights

- **Checkpointed Incremental Ingestion:** Tracks the highest timestamp exported (`last_export.txt`) and enforces a 5-second buffer window (`last < ts <= now - 5s`). This guarantees **exactly-once processing** across streaming bursts without splitting in-flight batches.
- **Tab-Separated Serialization:** Solves the common CSV parsing pitfall where movie titles contain commas (e.g., *"Shawshank Redemption, The (1994)"*).
- **External Tables in Apache Hive:** Uses `CREATE EXTERNAL TABLE` referencing HDFS directories. New files written by subsequent ingestion cycles are queryable immediately with zero manual schema reloading.
- **Minimum Support Thresholds:** Eliminates outlier distortion in ranking queries using SQL `HAVING COUNT(*) >= 10` so movies with a single 5.0 rating do not monopolize Top-10 leaderboards.
- **Automated Service Management:** Complete shell scripts (`start_all.sh`, `clear_data.sh`, `setup_wsl.sh`) handle environment configuration, daemons, and clean-slate resets.

---

## 🛠️ Technology Stack

| Component | Technology | Version | Role in Pipeline |
|---|---|---|---|
| **Storage (NoSQL)** | MongoDB | 8.0 | High-concurrency landing zone for live streaming ratings & dashboard stats cache |
| **Distributed Storage** | Apache Hadoop HDFS | 3.3.6 | Persistent, scalable storage for immutable rating batches |
| **Resource Manager** | Apache Hadoop YARN | 3.3.6 | Distributed job scheduling and cluster resource management |
| **Compute Engine** | Hadoop Streaming MapReduce | 3.3.6 | Distributed map and reduce execution using Python 3 |
| **Data Warehouse** | Apache Hive | 3.1.3 | Schema-on-read SQL analytics with embedded Derby metastore |
| **Programming Language** | Python | 3.10 | Producer streaming, ingestion loader, MapReduce jobs, stats sync |
| **Dashboard / Visualization** | Streamlit | 1.37+ | Real-time browser metrics, live charts, and KPI visualization |
| **Runtime Environment** | WSL2 / Ubuntu | 22.04 LTS | Virtualized Linux subsystem running all services on Windows |

---

## 📂 Repository Structure

```plaintext
├── docs/
│   ├── architecture.png        # Pipeline architectural diagram
│   ├── HOW_IT_WAS_BUILT.md     # Engineering design decisions, problems & solutions
│   ├── INSTALLATION.md         # Full step-by-step setup guide for WSL2/Ubuntu
│   └── RUNNING.md              # Live demonstration script & commands
├── results/
│   ├── hive_113127.txt         # Benchmark sample Hive query results
│   └── mapreduce_113127.tsv    # Benchmark sample MapReduce outputs
├── clear_data.sh               # Utility script to flush MongoDB & HDFS for a clean run
├── dashboard.py                # Streamlit live monitoring web dashboard
├── fix_env.sh                  # One-click recovery script for Hadoop/Hive/Java paths
├── mapper.py                   # MapReduce Mapper: emits (movieId, rating)
├── mongo_to_hdfs.py            # Incremental checkpoint ingestion: MongoDB -> HDFS
├── movie_analysis.hql          # 7 analytical Hive queries (Top-N, Genres, Trends)
├── producer.py                 # Live rating generator simulating user app traffic
├── reducer.py                  # MapReduce Reducer: computes count, sum, average per movie
├── requirements.txt            # Python dependencies (pymongo, streamlit, pandas)
├── run_pipeline.sh             # Master orchestrator: Ingest -> MapReduce -> Sync -> Hive
├── setup_wsl.sh                # Automated WSL2 system provisioning script
├── start_all.sh                # Helper script to start MongoDB, HDFS, and YARN
└── README.md                   # Project documentation
```

---

## 📊 Analytical Queries (Apache Hive)

The Hive script (`movie_analysis.hql`) executes 7 queries over external tables pointing to HDFS:

| Query | Objective | Key HiveQL Logic |
|---|---|---|
| **Q1** | **Ingestion Progress** | `COUNT(*)`, `COUNT(DISTINCT movie_id)`, `MIN(ts)`, `MAX(ts)` to measure live growth |
| **Q2** | **Top 10 Movies (Hive)** | `AVG(rating)` grouped by title `HAVING COUNT(*) >= 10` |
| **Q3** | **MapReduce Verification** | Joins MapReduce outputs (`mr_movie_stats`) with `movies` table to verify results match Q2 |
| **Q4** | **Most-Rated Movies** | Top movies by engagement volume |
| **Q5** | **Genre Breakdown** | `LATERAL VIEW explode(split(genres, '\\|'))` for normalized genre metrics |
| **Q6** | **Rating Distribution** | Frequency counts of ratings from 0.5 to 5.0 |
| **Q7** | **Arrival Velocity** | `GROUP BY date_format(ts, 'yyyy-MM-dd HH:mm')` measuring incoming velocity |

---

## ⚡ Quickstart Guide

### 1. Prerequisites
- **OS:** Windows 10/11 with WSL2 (Ubuntu 22.04) or native Linux
- **RAM:** Minimum 8 GB
- **Disk:** ~5 GB free disk space

### 2. Environment Setup
Run the automated installation script inside your Ubuntu 22.04 terminal:
```bash
bash setup_wsl.sh
```
*(Or follow the detailed step-by-step instructions in [docs/INSTALLATION.md](docs/INSTALLATION.md)).*

### 3. Download the Dataset
Download the [MovieLens ml-latest-small](https://grouplens.org/datasets/movielens/latest/) dataset into the project folder:
```bash
wget https://files.grouplens.org/datasets/movielens/ml-latest-small.zip
unzip ml-latest-small.zip
```

### 4. Start Daemons
```bash
./start_all.sh
```
Verify that `jps` outputs: `NameNode`, `DataNode`, `SecondaryNameNode`, `ResourceManager`, and `NodeManager`.

---

## 🖥️ Live Pipeline Execution

Open **3 separate terminal tabs** to observe the real-time pipeline in action:

#### Tab 1: Launch the Live Dashboard
```bash
python3 -m streamlit run dashboard.py --server.headless true
```
> Open **http://localhost:8501** in your browser.

#### Tab 2: Start the Live Rating Producer
```bash
# Streams 500 ratings every 3 seconds
python3 producer.py ml-latest-small 500 3
```

#### Tab 3: Trigger the Pipeline
Wait ~1 minute after starting the producer, then execute:
```bash
./run_pipeline.sh
```
Run `./run_pipeline.sh` periodically to watch HDFS ingest new batches, MapReduce recompute aggregates on YARN, and Hive update rankings in real time!

#### To Reset Data (Clean Slate):
```bash
./clear_data.sh
```

---

## 📈 Benchmark & Results Verification

During live pipeline runs across **46,100+ ratings**:
- **Dynamic Ingestion:** Rating count scaled seamlessly (600 → 5,100 → 46,100) with zero duplicates or data drops.
- **Cross-Engine Consistency:** The MapReduce job output on YARN matched Hive's independent SQL query aggregation with 100% precision.
- **Key Findings:**
  - **Highest Rated (min 10 ratings):** *The Shawshank Redemption (1994)* — **4.45** average rating (151 ratings).
  - **Most Rated:** *Forrest Gump (1994)* — 160 ratings.
  - **Top Performing Genre:** *Film-Noir* (3.95 avg), *War* (3.83 avg).
  - **Peak Arrival Rate:** ~10,000 ratings/minute handled smoothly across incremental batches.

---

## 🔧 Engineering Challenges & Solutions

| Issue Encountered | Root Cause | Resolution |
|---|---|---|
| **Hive startup failure with Hadoop 3.3.6** | ClassNotFound due to Guava version collision (`guava-19.0.jar` vs `guava-27.0-jre.jar`) | Replaced Hive's legacy Guava JAR with Hadoop's Guava 27 binary |
| **Java version incompatibility** | Apache Hive 3.1.3 requires Java 8; newer JDKs fail | Standardized cluster on `openjdk-8-jdk` via `JAVA_HOME` |
| **Derby metastore concurrency lock** | Relative path created locks across different execution directories | Configured absolute path `databaseName=$HOME/hive/metastore_db` in `hive-site.xml` |
| **MapReduce `MRAppMaster` missing on YARN** | Container environment missed `HADOOP_MAPRED_HOME` | Configured `yarn.app.mapreduce.am.env` in `mapred-site.xml` |
| **CSV delimiter collision in movie titles** | Titles containing commas break CSV splitting | Standardized entire pipeline on Tab-Separated Values (TSV) |

---

## 📄 Documentation Links
- 📘 [Full Installation & System Configuration Guide](docs/INSTALLATION.md)
- 🛠️ [Architecture & Engineering Design Decisions](docs/HOW_IT_WAS_BUILT.md)
- 🏃 [Runbook & Live Demo Execution](docs/RUNNING.md)

---

## 👥 Authors & Acknowledgments

- **Chirag (@itschiru07)** & **Nithelan Jayakumar** — Computer Science & Engineering, Gopalan College of Engineering & Management, Bengaluru.
- Built for Big Data Analytics (BCS714D).
- Dataset credit: GroupLens Research Project at the University of Minnesota.

---

## 📜 License
This project is open-source under the [MIT License](LICENSE).
