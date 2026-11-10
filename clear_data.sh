#!/usr/bin/env bash
cd "$(dirname "$0")"
echo "=== Resetting project data to zero ==="
mongosh movieapp --quiet --eval "db.dropDatabase()"
hdfs dfs -rm -r -f /bda/movies 2>/dev/null || true
rm -rf exports last_export.txt
mkdir -p results
echo "Clean slate ready! Ratings in MongoDB reset to 0."
