#!/usr/bin/env python3
"""Reducer. Input: movieId<TAB>rating (sorted by Hadoop)
Output: movieId<TAB>num_ratings<TAB>total_rating<TAB>avg_rating"""
import sys

current, total, count = None, 0.0, 0


def emit():
    print(f"{current}\t{count}\t{total:.1f}\t{total / count:.2f}")


for line in sys.stdin:
    try:
        movie_id, rating = line.strip().split("\t")
        rating = float(rating)
    except ValueError:
        continue
    if movie_id == current:
        total += rating
        count += 1
    else:
        if current is not None:
            emit()
        current, total, count = movie_id, rating, 1

if current is not None:
    emit()
