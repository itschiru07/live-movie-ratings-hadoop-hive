#!/usr/bin/env python3
"""
Live dashboard for the Movie Ratings Big Data pipeline.
  Live (MongoDB)    : totals, ratings per minute, latest ratings   -> refreshes every 5 s
  MapReduce results : movie_stats collection in MongoDB             -> after each pipeline run
  Hive results      : latest results/hive_*.txt                     -> after each pipeline run
Run: python3 -m streamlit run dashboard.py --server.headless true
"""
import glob
import os
import time
from datetime import timedelta

import pandas as pd
import streamlit as st
from pymongo import MongoClient

BASE = os.path.dirname(os.path.abspath(__file__))
REFRESH = 5
IST = timedelta(hours=5, minutes=30)

st.set_page_config(page_title="Live Movie Ratings", layout="wide")


@st.cache_resource
def get_db():
    return MongoClient("mongodb://localhost:27017")["movieapp"]


@st.cache_data(ttl=60)
def get_titles():
    return {m["movieId"]: m["title"]
            for m in get_db().movies.find({}, {"_id": 0, "movieId": 1, "title": 1})}


def latest_hive():
    files = sorted(glob.glob(os.path.join(BASE, "results", "hive_*.txt")), key=os.path.getmtime)
    if not files:
        return None, {}
    headers = {"total_ratings", "m.title", "genre", "rating", "minute"}
    sections, cur = {}, None
    with open(files[-1], encoding="utf-8") as f:
        for line in f:
            p = line.rstrip("\n").split("\t")
            if p[0] in headers:
                cur = p[0] if p[0] not in sections else None
                if cur:
                    sections[cur] = {"cols": p, "rows": []}
                continue
            if cur and len(p) == len(sections[cur]["cols"]):
                sections[cur]["rows"].append(p)
    tables = {k: pd.DataFrame(v["rows"], columns=v["cols"]) for k, v in sections.items()}
    return os.path.basename(files[-1]), tables


db = get_db()
titles = get_titles()

st.title("Live Movie Ratings - Big Data Pipeline")
st.caption("MongoDB (live data) -> HDFS -> MapReduce + Hive on YARN -> results back to MongoDB")

# ---------------- KPIs
total = db.ratings.estimated_document_count()
users = len(db.ratings.distinct("userId"))
movies_rated = len(db.ratings.distinct("movieId"))
last_stat = db.movie_stats.find_one(sort=[("updated_at", -1)])
last_run = (last_stat["updated_at"] + IST).strftime("%H:%M:%S") if last_stat else "not yet"

c1, c2, c3, c4 = st.columns(4)
c1.metric("Ratings in MongoDB", f"{total:,}")
c2.metric("Movies rated", f"{movies_rated:,}")
c3.metric("Users", f"{users:,}")
c4.metric("Last pipeline run", last_run)

# ---------------- Live data
st.subheader("Live data arriving (MongoDB)")
left, right = st.columns([2, 1])
per_min = list(db.ratings.aggregate([
    {"$group": {"_id": {"$dateToString": {"format": "%H:%M", "date": "$ts", "timezone": "+05:30"}},
                "ratings": {"$sum": 1}}},
    {"$sort": {"_id": 1}}]))
if per_min:
    df = pd.DataFrame(per_min).rename(columns={"_id": "minute"}).set_index("minute")
    left.markdown("**Ratings received per minute**")
    left.bar_chart(df["ratings"])
recent = list(db.ratings.find({}, {"_id": 0}).sort("ts", -1).limit(10))
if recent:
    r = pd.DataFrame(recent)
    r["movie"] = r["movieId"].map(titles)
    r["time"] = (r["ts"] + IST).dt.strftime("%H:%M:%S")
    right.markdown("**Latest 10 ratings**")
    right.dataframe(r[["time", "movie", "rating"]], hide_index=True)

# ---------------- MapReduce results
st.subheader("MapReduce results (movie_stats in MongoDB)")
stats = pd.DataFrame(list(db.movie_stats.find({}, {"_id": 0})))
if stats.empty:
    st.info("Run ./run_pipeline.sh to see MapReduce results.")
else:
    stats["title"] = stats["movieId"].map(titles)
    a, b = st.columns(2)
    top = (stats[stats["num_ratings"] >= 20]
           .sort_values(["avg_rating", "num_ratings"], ascending=False).head(10))
    a.markdown("**Top 10 highest-rated movies (min 20 ratings)**")
    a.dataframe(top[["title", "num_ratings", "avg_rating"]], hide_index=True)
    most = stats.sort_values("num_ratings", ascending=False).head(10)
    b.markdown("**Top 10 most-rated movies**")
    b.dataframe(most[["title", "num_ratings"]], hide_index=True,
                column_config={"num_ratings": st.column_config.ProgressColumn(
                    "Ratings", format="%d", min_value=0, max_value=int(most["num_ratings"].max()))})

# ---------------- Hive results
name, hive = latest_hive()
st.subheader(f"Hive analysis (from {name})" if name else "Hive analysis")
if not hive:
    st.info("Run ./run_pipeline.sh to see Hive results.")
else:
    a, b = st.columns(2)
    if "genre" in hive:
        g = (hive["genre"].astype({"num_ratings": int, "avg_rating": float})
             .sort_values("avg_rating", ascending=False))
        a.markdown("**Average rating by genre**")
        a.dataframe(g, hide_index=True,
                    column_config={"avg_rating": st.column_config.ProgressColumn(
                        "Avg rating", format="%.2f", min_value=0, max_value=5)})
    if "rating" in hive:
        d = hive["rating"].astype({"how_many": int}).set_index("rating")
        b.markdown("**Rating distribution**")
        b.bar_chart(d["how_many"])

st.caption(f"Live section refreshes every {REFRESH} seconds.")
time.sleep(REFRESH)
st.rerun()