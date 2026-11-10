#!/usr/bin/env python3
"""
Incremental Ingestion: MongoDB -> HDFS.
Copies ONLY new ratings that arrived since the last checkpoint into a new
timestamped TSV batch in HDFS. Preloads the movie catalogue on first run.

Guarantees:
  - Exactly-once processing via high-water mark timestamp state (last_export.txt).
  - 5-second buffer cutoff ensures no half-written streaming batches are split.
"""
import os
import shutil
import subprocess
import sys
from datetime import datetime, timedelta, timezone

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

IST = timezone(timedelta(hours=5, minutes=30))
HDFS_RATINGS = "/bda/movies/ratings"
HDFS_MOVIES = "/bda/movies/movies"
STATE = "last_export.txt"
OUT = "exports"


def check_prerequisites():
    if not shutil.which("hdfs"):
        sys.exit("❌ Error: 'hdfs' command not found. Have you sourced Hadoop environment variables?")
    try:
        client = MongoClient("mongodb://localhost:27017", serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        return client["movieapp"]
    except ConnectionFailure:
        sys.exit("❌ Error: Unable to connect to MongoDB at localhost:27017. Please start mongod.")


def hdfs_exec(*args):
    subprocess.run(["hdfs", "dfs", *args], check=True)


def main():
    db = check_prerequisites()
    os.makedirs(OUT, exist_ok=True)

    # 1. Sync movie catalogue if not present locally/HDFS
    movies_file = os.path.join(OUT, "movies.tsv")
    if not os.path.exists(movies_file):
        movie_count = db.movies.count_documents({})
        if movie_count == 0:
            sys.exit("❌ Error: 'movieapp.movies' collection is empty. Run producer.py first!")
        with open(movies_file, "w", encoding="utf-8") as f:
            for m in db.movies.find({}, {"_id": 0}).sort("movieId"):
                f.write(f"{m['movieId']}\t{m['title']}\t{m['genres']}\n")
        hdfs_exec("-mkdir", "-p", HDFS_MOVIES)
        hdfs_exec("-put", "-f", movies_file, HDFS_MOVIES + "/")
        print(f"🎬 Synced {movie_count:,} movies catalogue -> HDFS {HDFS_MOVIES}")

    # 2. Checkpoint recovery
    if os.path.exists(STATE):
        with open(STATE) as f:
            last = datetime.fromisoformat(f.read().strip())
    else:
        last = datetime(1970, 1, 1, tzinfo=timezone.utc)

    # 5-second buffer window to avoid in-flight write collisions
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=5)

    stamp = cutoff.astimezone(IST).strftime("%Y%m%d_%H%M%S")
    path = os.path.join(OUT, f"ratings_{stamp}.tsv")
    n = 0

    with open(path, "w", encoding="utf-8") as f:
        cursor = db.ratings.find({"ts": {"$gt": last, "$lte": cutoff}}, {"_id": 0}).sort("ts")
        for r in cursor:
            ts = r["ts"].replace(tzinfo=timezone.utc).astimezone(IST).strftime("%Y-%m-%d %H:%M:%S")
            f.write(f"{r['userId']}\t{r['movieId']}\t{r['rating']}\t{ts}\n")
            n += 1

    if n == 0:
        if os.path.exists(path):
            os.remove(path)
        print("ℹ️ No new ratings arrived since the last checkpoint.")
    else:
        hdfs_exec("-mkdir", "-p", HDFS_RATINGS)
        hdfs_exec("-put", "-f", path, HDFS_RATINGS + "/")
        print(f"🚀 Ingested {n:,} new ratings -> HDFS {HDFS_RATINGS}/{os.path.basename(path)}")

    with open(STATE, "w", encoding="utf-8") as f:
        f.write(cutoff.isoformat())


if __name__ == "__main__":
    main()
