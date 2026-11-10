# How This Project Was Built

A record of how the project was designed and built, step by step, including the problems we hit and how they were solved.

## 1. Choosing the problem
The BCS714D assignment lists 50 problems at three levels. **Q3 (Beginner, CO1 & CO2)** was chosen: *"Implement a MapReduce program to analyse a movie-rating dataset by calculating the total or average rating for each movie."*
MovieLens was picked because it is a clean, well-known dataset that matches the question exactly and is one of the recommended sources.

## 2. The faculty's extra requirements
1. **Not MapReduce alone** – Hadoop must be combined with Hive, Pig, Spark or cloud analytics → we added **Hive**.
2. **Dynamic data, not a static file, using MongoDB** → we built a producer that streams ratings into MongoDB and an incremental loader into HDFS.

## 3. Design
```
producer.py → MongoDB (live) → mongo_to_hdfs.py (only new data) → HDFS
            → MapReduce (Python, Hadoop Streaming) + Hive (on YARN)
            → results → MongoDB (movie_stats) → Streamlit dashboard
```
Design decisions:
- **Replay real ratings with live timestamps** – keeps the data real while making arrival dynamic. A fixed shuffle seed lets the producer stop and resume without duplicates.
- **Checkpointed incremental ingestion** – export `last < ts ≤ now − 5 s`, upload, then advance the checkpoint, so every rating reaches HDFS exactly once and a half-written batch is never split.
- **One new HDFS file per batch** – HDFS is write-once; MapReduce and Hive read every file in the folder.
- **Tab-separated files** – titles contain commas ("Shawshank Redemption, The (1994)").
- **Hive external tables** over the HDFS folders – new batches appear in queries automatically, without loading.
- **Cross-verification** – Hive computes the same averages independently (Q2) and is compared with the MapReduce output (Q3).

## 4. Build order (what was done, in order)
| Step | Work | Check |
|---|---|---|
| 1 | Separate Ubuntu-22.04 WSL distro | `lsb_release -a` |
| 2 | Java 8, SSH, Hadoop 3.3.6 configured and started | `jps` shows 5 daemons |
| 3 | Hive 3.1.3 with Derby metastore | `hive -e "show databases;"` |
| 4 | MongoDB 8.0 + PyMongo | `mongosh` ping `{ ok: 1 }` |
| 5 | Downloaded MovieLens ml-latest-small | 100,836 ratings |
| 6 | `producer.py` – tested 3 batches | 600 ratings in MongoDB |
| 7 | `mongo_to_hdfs.py` – run twice | 1 file in HDFS, 2nd run "No new ratings" |
| 8 | `mapper.py` / `reducer.py` – tested with Linux pipes, then on Hadoop | `_SUCCESS` + `part-00000` |
| 9 | `stats_to_mongo.py` | 537 movies in `movie_stats` |
| 10 | `movie_analysis.hql` – 7 queries | Q1 = 600 ratings |
| 11 | `run_pipeline.sh` + full live run (500 ratings / 3 s) | Q1: 5,100 → 46,100 between runs |
| 12 | `dashboard.py` (Streamlit) | http://localhost:8501 |

## 5. Problems solved along the way
| Problem | Cause | Fix |
|---|---|---|
| Hive would not start with Hadoop 3.3.6 | Guava 19 (Hive) vs Guava 27 (Hadoop) | Replace Hive's `guava-19.0.jar` with Hadoop's `guava-27.0-jre.jar` |
| Hive incompatibility with newer Java | Hive 3.1.3 targets Java 8 | Use OpenJDK 8 for the whole stack |
| Metastore created in random folders | Relative Derby path | Absolute `databaseName=$HOME/hive/metastore_db` |
| MapReduce "cannot find MRAppMaster" (prevented) | YARN containers lack `HADOOP_MAPRED_HOME` | Set it in `mapred-site.xml` |
| Commas in movie titles | CSV splitting | Write TSV to HDFS |
| Top-10 dominated by 1-rating movies | No minimum support | `HAVING COUNT(*) >= 10` |
| Opened the wrong Linux shell | Docker Desktop's `docker-desktop` distro | `wsl -d Ubuntu-22.04` and `wsl --set-default` |

## 6. Results
See the main [README](../README.md): MapReduce Top 10 = Hive Top 10; Shawshank Redemption 4.45; Forrest Gump most rated; Film-Noir best genre; 4.0 most common rating; ~10,000 ratings/minute during the live run.
