#!/usr/bin/env bash
# ========================================================
# Helper script to download and unpack MovieLens 100k
# ========================================================
set -e

DIR="ml-latest-small"
ZIP="ml-latest-small.zip"
URL="https://files.grouplens.org/datasets/movielens/ml-latest-small.zip"

if [ -d "$DIR" ] && [ -f "$DIR/ratings.csv" ] && [ -f "$DIR/movies.csv" ]; then
    echo "✅ Dataset '$DIR' is already downloaded and verified."
    exit 0
fi

echo "📥 Downloading MovieLens 100k dataset..."
curl -fsSL -o "$ZIP" "$URL" || wget -c "$URL" -O "$ZIP"

echo "📦 Extracting $ZIP..."
unzip -q -o "$ZIP"
rm -f "$ZIP"

echo "✅ Dataset ready in ./$DIR"
