#!/usr/bin/env bash
echo "=== Starting MongoDB ==="
sudo systemctl start mongod 2>/dev/null || sudo service mongod start 2>/dev/null || true

echo "=== Starting Hadoop (HDFS + YARN) ==="
start-dfs.sh
start-yarn.sh

echo "=== Status Check ==="
jps
mongosh --quiet --eval "db.runCommand({ping:1})"
echo "All Big Data services are running!"
