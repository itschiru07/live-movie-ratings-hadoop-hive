#!/bin/bash
# One cycle: MongoDB -> HDFS -> MapReduce -> MongoDB -> Hive
set -e
cd "$(dirname "$0")"
STREAMING_JAR=$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar
RUN=$(date +%H%M%S)

echo "===== 1. New data: MongoDB -> HDFS ====="
python3 mongo_to_hdfs.py
hdfs dfs -ls /bda/movies/ratings

echo "===== 2. MapReduce: total & average rating per movie ====="
hdfs dfs -rm -r -f /bda/movies/mr_output
hadoop jar "$STREAMING_JAR" \
  -files mapper.py,reducer.py \
  -mapper "python3 mapper.py" \
  -reducer "python3 reducer.py" \
  -input /bda/movies/ratings \
  -output /bda/movies/mr_output
hdfs dfs -cat /bda/movies/mr_output/part-* > results/mapreduce_$RUN.tsv
echo "MapReduce output saved to results/mapreduce_$RUN.tsv"

echo "===== 3. Results back to MongoDB ====="
hdfs dfs -cat /bda/movies/mr_output/part-* | python3 stats_to_mongo.py

echo "===== 4. Hive analysis ====="
hive -S -f movie_analysis.hql | tee results/hive_$RUN.txt
echo "===== Cycle done. Hive results saved to results/hive_$RUN.txt ====="
