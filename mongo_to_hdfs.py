#!/usr/bin/env python3
"""
Incremental ingestion: MongoDB -> HDFS.
Each run copies ONLY the ratings that arrived since the last run into a new
HDFS file. The movie catalogue is copied once.
"""
import os
import subprocess
from datetime import datetime, timedelta, timezone

from pymongo import MongoClient

IST = timezone(timedelta(hours=5, minutes=30))
HDFS_RATINGS = "/bda/movies/ratings"
HDFS_MOVIES = "/bda/movies/movies"
STATE = "last_export.txt"
OUT = "exports"


def hdfs(*args):
    subprocess.run(["hdfs", "dfs", *args], check=True)


db = MongoClient("mongodb://localhost:27017")["movieapp"]
os.makedirs(OUT, exist_ok=True)

movies_file = f"{OUT}/movies.tsv"
if not os.path.exists(movies_file):
    with open(movies_file, "w", encoding="utf-8") as f:
        for m in db.movies.find({}, {"_id": 0}).sort("movieId"):
            f.write(f"{m['movieId']}\t{m['title']}\t{m['genres']}\n")
    hdfs("-mkdir", "-p", HDFS_MOVIES)
    hdfs("-put", "-f", movies_file, HDFS_MOVIES + "/")
    print(f"Movie catalogue copied to HDFS {HDFS_MOVIES}")

if os.path.exists(STATE):
    with open(STATE) as f:
        last = datetime.fromisoformat(f.read().strip())
else:
    last = datetime(1970, 1, 1, tzinfo=timezone.utc)
cutoff = datetime.now(timezone.utc) - timedelta(seconds=5)   # skip a batch still being written

stamp = cutoff.astimezone(IST).strftime("%Y%m%d_%H%M%S")
path = f"{OUT}/ratings_{stamp}.tsv"
n = 0
with open(path, "w") as f:
    for r in db.ratings.find({"ts": {"$gt": last, "$lte": cutoff}}, {"_id": 0}).sort("ts"):
        ts = r["ts"].replace(tzinfo=timezone.utc).astimezone(IST).strftime("%Y-%m-%d %H:%M:%S")
        f.write(f"{r['userId']}\t{r['movieId']}\t{r['rating']}\t{ts}\n")
        n += 1

if n == 0:
    os.remove(path)
    print("No new ratings since last export.")
else:
    hdfs("-mkdir", "-p", HDFS_RATINGS)
    hdfs("-put", "-f", path, HDFS_RATINGS + "/")
    print(f"Exported {n} new ratings -> HDFS {HDFS_RATINGS}/{os.path.basename(path)}")

with open(STATE, "w") as f:
    f.write(cutoff.isoformat())
