#!/usr/bin/env python3
"""Mapper. Input: userId<TAB>movieId<TAB>rating<TAB>ts  ->  Output: movieId<TAB>rating"""
import sys

for line in sys.stdin:
    parts = line.strip().split("\t")
    if len(parts) != 4:
        continue
    user_id, movie_id, rating, ts = parts
    try:
        rating = float(rating)
    except ValueError:
        continue
    print(f"{movie_id}\t{rating}")
