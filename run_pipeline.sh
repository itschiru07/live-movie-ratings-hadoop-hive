#!/usr/bin/env bash
# ==============================================================================
# Master Pipeline Orchestrator:
# Ingest (MongoDB -> HDFS) -> Compute (MapReduce on YARN) -> Sync (MongoDB) -> Hive SQL
# ==============================================================================
set -e
cd "$(dirname "$0")"

# 1. Validate Hadoop and Hive environment
if ! command -v hdfs &> /dev/null || ! command -v hive &> /dev/null; then
    echo "❌ Error: Hadoop or Hive binaries not found in PATH."
    echo "👉 Ensure you have run: source ~/.bashrc && bash start_all.sh"
    exit 1
fi

# Locate hadoop-streaming JAR dynamically across versions
STREAMING_JAR=$(find "$HADOOP_HOME/share/hadoop/tools/lib/" -name "hadoop-streaming*.jar" 2>/dev/null | head -n 1)
if [ -z "$STREAMING_JAR" ]; then
    echo "❌ Error: Hadoop Streaming JAR not found under \$HADOOP_HOME/share/hadoop/tools/lib/"
    exit 1
fi

mkdir -p results exports
RUN=$(date +%H%M%S)

echo "=========================================================="
echo " Starting Pipeline Run: $RUN"
echo " Streaming Jar: $(basename "$STREAMING_JAR")"
echo "=========================================================="

echo "===== Step 1: Incremental Ingestion (MongoDB -> HDFS) ====="
python3 mongo_to_hdfs.py
hdfs dfs -ls /bda/movies/ratings || true

echo "===== Step 2: Distributed MapReduce on YARN ====="
hdfs dfs -rm -r -f /bda/movies/mr_output
hadoop jar "$STREAMING_JAR" \
  -files mapper.py,reducer.py \
  -mapper "python3 mapper.py" \
  -reducer "python3 reducer.py" \
  -input /bda/movies/ratings \
  -output /bda/movies/mr_output

hdfs dfs -cat /bda/movies/mr_output/part-* > "results/mapreduce_$RUN.tsv"
echo "✅ MapReduce results saved to results/mapreduce_$RUN.tsv"

echo "===== Step 3: Sync MapReduce Aggregations to MongoDB ====="
hdfs dfs -cat /bda/movies/mr_output/part-* | python3 stats_to_mongo.py

echo "===== Step 4: Analytical Deep-Dive (Apache Hive) ====="
hive -S -f movie_analysis.hql | tee "results/hive_$RUN.txt"

echo "=========================================================="
echo " 🎉 Pipeline Cycle Completed Successfully!"
echo " Results stored in: results/mapreduce_$RUN.tsv & results/hive_$RUN.txt"
echo " View in Dashboard: python3 -m streamlit run dashboard.py"
echo "=========================================================="
