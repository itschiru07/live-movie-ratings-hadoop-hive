#!/usr/bin/env python3
"""
Live Data Producer: Simulates real-time user rating activity.
Continuously streams ratings into MongoDB with realistic timestamps.

Collections:
  - movies  : movieId, title, genres (preloaded catalogue)
  - ratings : userId, movieId, rating, ts (appended in real-time)

Usage:
  python3 producer.py [dataset_dir] [batch_size] [interval_seconds]
  Example: python3 producer.py ml-latest-small 500 3
"""
import csv
import os
import random
import sys
import time
from datetime import datetime, timezone

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

DATA = sys.argv[1] if len(sys.argv) > 1 else "ml-latest-small"
BATCH = int(sys.argv[2]) if len(sys.argv) > 2 else 200
INTERVAL = float(sys.argv[3]) if len(sys.argv) > 3 else 5

movies_csv = os.path.join(DATA, "movies.csv")
ratings_csv = os.path.join(DATA, "ratings.csv")

if not os.path.exists(movies_csv) or not os.path.exists(ratings_csv):
    sys.exit(
        f"❌ Error: Dataset files not found in '{DATA}'.\n"
        f"👉 Download the dataset using: bash download_dataset.sh"
    )

try:
    client = MongoClient("mongodb://localhost:27017", serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    db = client["movieapp"]
except ConnectionFailure:
    sys.exit("❌ Error: Unable to connect to MongoDB at localhost:27017. Please run: bash start_all.sh")

if db.movies.count_documents({}) == 0:
    with open(movies_csv, newline="", encoding="utf-8") as f:
        movies = [
            {"movieId": int(r["movieId"]), "title": r["title"], "genres": r["genres"]}
            for r in csv.DictReader(f)
        ]
    db.movies.insert_many(movies)
    db.movies.create_index("movieId", unique=True)
    print(f"🎬 Preloaded {len(movies)} movies into MongoDB 'movieapp.movies'")

db.ratings.create_index("ts")

with open(ratings_csv, newline="", encoding="utf-8") as f:
    rows = list(csv.DictReader(f))

# Fixed seed ensures deterministic order across restarts without duplicate re-runs
random.Random(42).shuffle(rows)

i = db.ratings.count_documents({})
print(f"📊 {len(rows):,} total ratings in dataset | {i:,} already streamed.")
print(f"🚀 Streaming {BATCH} ratings every {INTERVAL}s (Press Ctrl+C to pause)...")

try:
    while i < len(rows):
        now = datetime.now(timezone.utc)
        batch = rows[i : i + BATCH]
        documents = [
            {
                "userId": int(r["userId"]),
                "movieId": int(r["movieId"]),
                "rating": float(r["rating"]),
                "ts": now,
            }
            for r in batch
        ]
        db.ratings.insert_many(documents)
        i += len(batch)
        print(f"[{datetime.now():%H:%M:%S}] +{len(batch):>4} ratings | Total in MongoDB: {i:,}")
        time.sleep(INTERVAL)
    print("🎉 All ratings streamed successfully!")
except KeyboardInterrupt:
    print(f"\n⏸️ Producer paused. Total ratings in MongoDB: {i:,}")
