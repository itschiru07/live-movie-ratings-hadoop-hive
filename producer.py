#!/usr/bin/env python3
"""
Live data source: simulates a movie app where users keep rating movies.
Every rating is written to MongoDB as it happens.
  movies  collection : movieId, title, genres      (loaded once)
  ratings collection : userId, movieId, rating, ts  (grows every few seconds)
Usage: python3 producer.py ml-latest-small 200 5
Stop with Ctrl+C. Run again later and it continues where it stopped.
"""
import csv
import random
import sys
import time
from datetime import datetime, timezone

from pymongo import MongoClient

DATA = sys.argv[1] if len(sys.argv) > 1 else "ml-latest-small"
BATCH = int(sys.argv[2]) if len(sys.argv) > 2 else 200
INTERVAL = float(sys.argv[3]) if len(sys.argv) > 3 else 5

db = MongoClient("mongodb://localhost:27017")["movieapp"]

if db.movies.count_documents({}) == 0:
    with open(f"{DATA}/movies.csv", newline="", encoding="utf-8") as f:
        movies = [{"movieId": int(r["movieId"]), "title": r["title"], "genres": r["genres"]}
                  for r in csv.DictReader(f)]
    db.movies.insert_many(movies)
    db.movies.create_index("movieId", unique=True)
    print(f"Loaded {len(movies)} movies into MongoDB")

db.ratings.create_index("ts")

with open(f"{DATA}/ratings.csv", newline="", encoding="utf-8") as f:
    rows = list(csv.DictReader(f))
random.Random(42).shuffle(rows)          # fixed order, so a restart resumes without duplicates

i = db.ratings.count_documents({})
print(f"{len(rows)} ratings available, {i} already streamed. Streaming {BATCH} every {INTERVAL}s ...")

try:
    while i < len(rows):
        now = datetime.now(timezone.utc)
        batch = rows[i:i + BATCH]
        db.ratings.insert_many([{"userId": int(r["userId"]), "movieId": int(r["movieId"]),
                                 "rating": float(r["rating"]), "ts": now} for r in batch])
        i += len(batch)
        print(f"[{datetime.now():%H:%M:%S}] +{len(batch)} ratings | total in MongoDB: {i}")
        time.sleep(INTERVAL)
    print("All ratings streamed.")
except KeyboardInterrupt:
    print(f"\nStopped. {i} ratings in MongoDB.")
