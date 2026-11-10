# Running the Project

Copy-paste these commands into Ubuntu. They are the same commands used for the demo.

```text
STEP 0 - START EVERYTHING (every time you open Ubuntu)
---------------------------------------------------------------------
cd ~
sudo service ssh start
start-dfs.sh && start-yarn.sh
jps

  -> jps must show: NameNode, DataNode, SecondaryNameNode,
     ResourceManager, NodeManager

sudo systemctl start mongod
mongosh --quiet --eval "db.runCommand({ping:1})"

  -> must print: { ok: 1 }

hive -e "show databases;"

  -> warms up Hive (takes ~30 sec), must print: default

Browser pages (open in Chrome):
  HDFS : http://localhost:9870
  YARN : http://localhost:8088
```

## Project I - Live movie ratings
```text
PROJECT I - LIVE MOVIE RATINGS (MapReduce + Hive)
=====================================================================

---- 1. CLEAR (start again from zero) ----
cd ~/bda/p3_movie_live
mongosh movieapp --quiet --eval "db.dropDatabase()"
hdfs dfs -rm -r -f /bda/movies
rm -rf exports last_export.txt results/*
mkdir -p results

---- 2. RUN (open 3 Ubuntu tabs, run each line in its own tab) ----

TAB 1 - Dashboard (leave running):
cd ~/bda/p3_movie_live
python3 -m streamlit run dashboard.py --server.headless true

  -> open http://localhost:8501 in Chrome

TAB 2 - Live data producer (leave running, ~10 min for all ratings):
cd ~/bda/p3_movie_live
python3 producer.py ml-latest-small 500 3

TAB 3 - Pipeline (wait 1 minute after starting the producer):
cd ~/bda/p3_movie_live
./run_pipeline.sh

  -> takes 5-7 min. Run ./run_pipeline.sh AGAIN 2-3 times:
     once more while the producer is running,
     and once after the producer prints "All ratings streamed."

---- 3. VIEW RESULTS ----
cd ~/bda/p3_movie_live
ls -lh results/
cat $(ls -t results/hive_*.txt | head -1)
for f in results/hive_*.txt; do echo "== $f"; head -2 "$f"; done
head $(ls -t results/mapreduce_*.tsv | head -1)
hdfs dfs -ls /bda/movies/ratings
mongosh movieapp --quiet --eval "db.ratings.countDocuments()"
mongosh movieapp --quiet --eval "db.movie_stats.find({num_ratings:{\$gte:50}},{_id:0}).sort({avg_rating:-1}).limit(5)"
explorer.exe results

  What to point out:
  - Q1 total_ratings grows every run  (proof of dynamic data)
  - Q2 (Hive) and Q3 (MapReduce) Top 10 are identical (proof MapReduce is correct)
```

## Stop
```text
STOP EVERYTHING
=====================================================================
(Ctrl+C in every tab that is running a dashboard or producer)
stop-yarn.sh && stop-dfs.sh

In PowerShell, to shut the whole Ubuntu down:
wsl --shutdown


=====================================================================
 IMPORTANT
=====================================================================
- NEVER run "hdfs namenode -format" again (it erases HDFS).
- Only one Hive session can run at a time (close other hive windows).
- "No new ratings/events since last export" = normal, producer is stopped.
- Saved results stay in each project's results/ folder.
```
