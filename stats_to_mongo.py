#!/usr/bin/env python3
"""
Stores MapReduce aggregation results into MongoDB (movieapp.movie_stats).
Enables the live dashboard to query up-to-date movie statistics immediately.

Usage:
  hdfs dfs -cat /bda/movies/mr_output/part-* | python3 stats_to_mongo.py
"""
import sys
from datetime import datetime, timezone

from pymongo import MongoClient, UpdateOne
from pymongo.errors import ConnectionFailure, PyMongoError


def main():
    try:
        client = MongoClient("mongodb://localhost:27017", serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        db = client["movieapp"]
    except ConnectionFailure:
        sys.exit("❌ Error: Unable to connect to MongoDB at localhost:27017. Is mongod running?")

    now = datetime.now(timezone.utc)
    ops = []

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        parts = line.split("\t")
        if len(parts) != 4:
            continue

        movie_id, count, total, avg = parts
        try:
            ops.append(
                UpdateOne(
                    {"movieId": int(movie_id)},
                    {
                        "$set": {
                            "num_ratings": int(count),
                            "total_rating": float(total),
                            "avg_rating": float(avg),
                            "updated_at": now,
                        }
                    },
                    upsert=True,
                )
            )
        except ValueError:
            continue

    if ops:
        try:
            db.movie_stats.bulk_write(ops)
            print(f"✅ Successfully updated {len(ops)} movies in MongoDB collection 'movie_stats'")
        except PyMongoError as e:
            sys.exit(f"❌ Error during MongoDB bulk write: {e}")
    else:
        print("⚠️ No valid records found to update in MongoDB.")


if __name__ == "__main__":
    main()
