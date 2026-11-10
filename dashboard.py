#!/usr/bin/env python3
"""
Live Dashboard for Big Data Movie Ratings Analytics Pipeline.
Visualizes real-time MongoDB streaming data, distributed MapReduce outputs,
and multi-dimensional Apache Hive analytics queries.

Run with:
    python3 -m streamlit run dashboard.py --server.headless true
"""
import glob
import os
import time
from datetime import timedelta

import pandas as pd
import streamlit as st
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError

BASE = os.path.dirname(os.path.abspath(__file__))
IST = timedelta(hours=5, minutes=30)

st.set_page_config(
    page_title="Live Movie Ratings — Big Data Analytics",
    page_icon="🎬",
    layout="wide",
    initial_sidebar_state="expanded",
)


@st.cache_resource
def get_mongo_client():
    try:
        client = MongoClient("mongodb://localhost:27017", serverSelectionTimeoutMS=2000)
        client.admin.command("ping")
        return client
    except (ConnectionFailure, ServerSelectionTimeoutError):
        return None


def get_db():
    client = get_mongo_client()
    return client["movieapp"] if client else None


@st.cache_data(ttl=60)
def get_titles():
    db = get_db()
    if db is None:
        return {}
    try:
        return {
            m["movieId"]: m["title"]
            for m in db.movies.find({}, {"_id": 0, "movieId": 1, "title": 1})
        }
    except Exception:
        return {}


def load_latest_hive_results():
    pattern = os.path.join(BASE, "results", "hive_*.txt")
    files = sorted(glob.glob(pattern), key=os.path.getmtime)
    if not files:
        return None, {}

    latest_file = files[-1]
    headers = {"total_ratings", "m.title", "genre", "rating", "minute"}
    sections = {}
    current_key = None

    try:
        with open(latest_file, encoding="utf-8") as f:
            for raw_line in f:
                parts = raw_line.rstrip("\n").split("\t")
                if not parts or not parts[0]:
                    continue
                if parts[0] in headers:
                    current_key = parts[0]
                    if current_key not in sections:
                        sections[current_key] = {"cols": parts, "rows": []}
                    continue
                if current_key and len(parts) == len(sections[current_key]["cols"]):
                    sections[current_key]["rows"].append(parts)

        tables = {
            k: pd.DataFrame(v["rows"], columns=v["cols"])
            for k, v in sections.items()
            if v["rows"]
        }
        return os.path.basename(latest_file), tables
    except Exception as err:
        st.warning(f"Unable to parse Hive results: {err}")
        return None, {}


# --- Sidebar ---
st.sidebar.title("🎬 Pipeline Control")
client = get_mongo_client()
if client:
    st.sidebar.success("🟢 MongoDB: Connected")
else:
    st.sidebar.error("🔴 MongoDB: Disconnected")
    st.sidebar.caption("Run `bash start_all.sh` to start MongoDB and Hadoop.")

auto_refresh = st.sidebar.checkbox("Auto-refresh Live Data", value=True)
refresh_interval = st.sidebar.slider("Refresh Rate (seconds)", min_value=3, max_value=30, value=5)
min_reviews = st.sidebar.slider("Min Ratings Filter (MapReduce)", min_value=5, max_value=100, value=20)

if st.sidebar.button("🔄 Refresh Now"):
    st.rerun()

st.sidebar.markdown("---")
st.sidebar.markdown("### 🛠️ Tech Stack")
st.sidebar.markdown("- **Hadoop:** 3.3.6 (HDFS & YARN)\n- **Hive:** 3.1.3\n- **MongoDB:** 8.0\n- **Compute:** MapReduce Streaming\n- **UI:** Streamlit & Pandas")

# --- Header ---
st.title("🎬 Real-Time Movie Rating Analytics Pipeline")
st.caption("Live MongoDB Ingestion ➔ HDFS Checkpoints ➔ Hadoop MapReduce (YARN) ➔ Apache Hive ➔ Streamlit Dashboard")

db = get_db()
if db is None:
    st.error("⚠️ Cannot connect to MongoDB at `localhost:27017`.")
    st.info("Please make sure MongoDB is running. You can run `bash start_all.sh` in your Ubuntu terminal.")
    st.stop()

titles = get_titles()

# --- Top KPIs ---
total_ratings = db.ratings.estimated_document_count()
users_count = len(db.ratings.distinct("userId"))
movies_rated_count = len(db.ratings.distinct("movieId"))
mr_movies_computed = db.movie_stats.count_documents({})
last_stat = db.movie_stats.find_one(sort=[("updated_at", -1)])
last_run_time = (last_stat["updated_at"] + IST).strftime("%H:%M:%S") if last_stat else "Pending"

kpi1, kpi2, kpi3, kpi4, kpi5 = st.columns(5)
kpi1.metric("Live Ratings (MongoDB)", f"{total_ratings:,}")
kpi2.metric("Movies Rated", f"{movies_rated_count:,}")
kpi3.metric("Active Users", f"{users_count:,}")
kpi4.metric("MapReduce Aggregates", f"{mr_movies_computed:,}")
kpi5.metric("Last Pipeline Cycle", last_run_time)

st.markdown("---")

# --- Tabs ---
tab1, tab2, tab3, tab4 = st.tabs([
    "🔴 Live Ingestion Stream",
    "⚡ MapReduce Aggregations",
    "🐝 Apache Hive Insights",
    "🏗️ Architecture & Commands",
])

# Tab 1: Live Stream
with tab1:
    st.subheader("Live Ratings Arriving in MongoDB")
    col_chart, col_feed = st.columns([3, 2])

    with col_chart:
        st.markdown("**Ratings Ingested Per Minute**")
        pipeline = [
            {
                "$group": {
                    "_id": {
                        "$dateToString": {
                            "format": "%H:%M",
                            "date": "$ts",
                            "timezone": "+05:30",
                        }
                    },
                    "count": {"$sum": 1},
                }
            },
            {"$sort": {"_id": 1}},
        ]
        trend_data = list(db.ratings.aggregate(pipeline))
        if trend_data:
            df_trend = pd.DataFrame(trend_data).rename(columns={"_id": "Minute", "count": "Ratings"}).set_index("Minute")
            st.bar_chart(df_trend["Ratings"], color="#F09A37")
        else:
            st.info("No ratings recorded yet. Start the producer with: `python3 producer.py ml-latest-small 500 3`")

    with col_feed:
        st.markdown("**Real-Time Stream Feed (Latest 10)**")
        recent = list(db.ratings.find({}, {"_id": 0}).sort("ts", -1).limit(10))
        if recent:
            df_recent = pd.DataFrame(recent)
            df_recent["Movie"] = df_recent["movieId"].map(titles)
            df_recent["Time (IST)"] = (df_recent["ts"] + IST).dt.strftime("%H:%M:%S")
            st.dataframe(
                df_recent[["Time (IST)", "Movie", "rating"]].rename(columns={"rating": "Rating ⭐"}),
                hide_index=True,
                use_container_width=True,
            )
        else:
            st.write("Awaiting live stream...")

# Tab 2: MapReduce Results
with tab2:
    st.subheader("MapReduce Aggregations on YARN (`movieapp.movie_stats`)")
    mr_data = list(db.movie_stats.find({}, {"_id": 0}))

    if not mr_data:
        st.info("💡 MapReduce results not found in MongoDB yet. Run `./run_pipeline.sh` in your terminal.")
    else:
        df_mr = pd.DataFrame(mr_data)
        df_mr["Title"] = df_mr["movieId"].map(titles)

        col_top, col_most = st.columns(2)

        with col_top:
            st.markdown(f"**Top 10 Highest-Rated Movies (≥ {min_reviews} ratings)**")
            top_movies = (
                df_mr[df_mr["num_ratings"] >= min_reviews]
                .sort_values(by=["avg_rating", "num_ratings"], ascending=False)
                .head(10)
            )
            st.dataframe(
                top_movies[["Title", "avg_rating", "num_ratings"]].rename(
                    columns={"avg_rating": "Average Rating", "num_ratings": "Total Reviews"}
                ),
                hide_index=True,
                use_container_width=True,
                column_config={
                    "Average Rating": st.column_config.ProgressColumn(
                        "Avg Rating", format="%.2f", min_value=1.0, max_value=5.0
                    )
                },
            )

        with col_most:
            st.markdown("**Top 10 Most-Rated Movies (Engagement)**")
            most_rated = df_mr.sort_values(by="num_ratings", ascending=False).head(10)
            max_val = int(most_rated["num_ratings"].max()) if not most_rated.empty else 100
            st.dataframe(
                most_rated[["Title", "num_ratings", "avg_rating"]].rename(
                    columns={"num_ratings": "Review Count", "avg_rating": "Avg Rating"}
                ),
                hide_index=True,
                use_container_width=True,
                column_config={
                    "Review Count": st.column_config.ProgressColumn(
                        "Review Count", format="%d", min_value=0, max_value=max_val
                    )
                },
            )

# Tab 3: Apache Hive Insights
with tab3:
    latest_name, hive_tables = load_latest_hive_results()
    st.subheader(f"Apache Hive Multi-Dimensional Analytics ({latest_name or 'No run output yet'})")

    if not hive_tables:
        st.info("💡 Hive analytical query outputs not detected in `results/`. Run `./run_pipeline.sh` to generate them.")
    else:
        hive_col1, hive_col2 = st.columns(2)

        if "genre" in hive_tables:
            with hive_col1:
                st.markdown("**Average Rating by Genre (Q5)**")
                df_genre = (
                    hive_tables["genre"]
                    .astype({"num_ratings": int, "avg_rating": float})
                    .sort_values("avg_rating", ascending=False)
                )
                st.dataframe(
                    df_genre.rename(columns={"genre": "Genre", "num_ratings": "Ratings", "avg_rating": "Avg Rating"}),
                    hide_index=True,
                    use_container_width=True,
                    column_config={
                        "Avg Rating": st.column_config.ProgressColumn(
                            "Avg Rating", format="%.2f", min_value=1.0, max_value=5.0
                        )
                    },
                )

        if "rating" in hive_tables:
            with hive_col2:
                st.markdown("**Rating Frequency Distribution (Q6)**")
                df_dist = hive_tables["rating"].astype({"how_many": int})
                df_dist = df_dist.set_index("rating")
                st.bar_chart(df_dist["how_many"], color="#47A248")

        if "minute" in hive_tables:
            st.markdown("**Batch Arrival Trends Per Minute (Q7)**")
            df_arrival = hive_tables["minute"].astype({"new_ratings": int, "avg_rating": float})
            st.line_chart(df_arrival.set_index("minute")[["new_ratings", "avg_rating"]])

# Tab 4: Architecture & Commands
with tab4:
    st.markdown("### 🏗️ Pipeline Flow Architecture")
    st.code(
        """
[ MovieLens Dataset (100k) ]
            │
            ▼
   [ producer.py ] ──────► MongoDB (movieapp.ratings)
                                │
                                ▼ (Checkpoint Incremental Batch)
                       [ mongo_to_hdfs.py ] ──────► HDFS (/bda/movies/ratings)
                                                          │
                        ┌─────────────────────────────────┴─────────────────────────────┐
                        ▼                                                               ▼
        [ Hadoop Streaming MapReduce ]                                          [ Apache Hive ]
        (mapper.py & reducer.py on YARN)                                     (External Tables / HQL)
                        │                                                               │
                        ▼                                                               ▼
             MongoDB (movie_stats)                                                results/hive_*.txt
                        │                                                               │
                        └───────────────────────┬───────────────────────────────────────┘
                                                ▼
                                    [ Streamlit Dashboard ]
        """,
        language="text",
    )
    st.markdown("### 🏃 Quick Terminal Commands")
    st.code(
        """
# Tab 1: Start Services & Live Dashboard
bash start_all.sh
python3 -m streamlit run dashboard.py --server.headless true

# Tab 2: Stream Live Ratings
python3 producer.py ml-latest-small 500 3

# Tab 3: Run Analytics Cycle
bash run_pipeline.sh

# Reset Everything (Clean Slate)
bash clear_data.sh
        """,
        language="bash",
    )

st.caption(f"⚡ Dashboard auto-refresh is {'ACTIVE' if auto_refresh else 'PAUSED'} (every {refresh_interval}s).")

if auto_refresh:
    time.sleep(refresh_interval)
    st.rerun()
