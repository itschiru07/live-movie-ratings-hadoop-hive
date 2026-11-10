#!/usr/bin/env python3
"""Stores the MapReduce results in MongoDB (movie_stats) so the app can show live averages.
Usage: hdfs dfs -cat /bda/movies/mr_output/part-* | python3 stats_to_mongo.py"""
import sys
from datetime import datetime, timezone

from pymongo import MongoClient, UpdateOne

db = MongoClient("mongodb://localhost:27017")["movieapp"]
now = datetime.now(timezone.utc)
ops = []
for line in sys.stdin:
    movie_id, count, total, avg = line.strip().split("\t")
    ops.append(UpdateOne({"movieId": int(movie_id)},
                         {"$set": {"num_ratings": int(count), "total_rating": float(total),
                                   "avg_rating": float(avg), "updated_at": now}},
                         upsert=True))
if ops:
    db.movie_stats.bulk_write(ops)
print(f"Updated {len(ops)} movies in MongoDB collection movie_stats")
