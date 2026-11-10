CREATE DATABASE IF NOT EXISTS movieapp;
USE movieapp;

-- External tables point at HDFS folders, so new files from MongoDB appear automatically
CREATE EXTERNAL TABLE IF NOT EXISTS ratings (
  user_id INT, movie_id INT, rating DOUBLE, ts TIMESTAMP)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE LOCATION '/bda/movies/ratings';

CREATE EXTERNAL TABLE IF NOT EXISTS movies (
  movie_id INT, title STRING, genres STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE LOCATION '/bda/movies/movies';

CREATE EXTERNAL TABLE IF NOT EXISTS mr_movie_stats (
  movie_id INT, num_ratings INT, total_rating DOUBLE, avg_rating DOUBLE)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE LOCATION '/bda/movies/mr_output';

-- Q1: How much live data has arrived so far
SELECT COUNT(*) AS total_ratings, COUNT(DISTINCT movie_id) AS movies_rated,
       COUNT(DISTINCT user_id) AS users, MIN(ts) AS first_rating, MAX(ts) AS latest_rating
FROM ratings;

-- Q2: Total and average rating per movie (Top 10, at least 10 ratings)
SELECT m.title, COUNT(*) AS num_ratings, ROUND(SUM(r.rating), 1) AS total_rating,
       ROUND(AVG(r.rating), 2) AS avg_rating
FROM ratings r JOIN movies m ON r.movie_id = m.movie_id
GROUP BY m.title HAVING COUNT(*) >= 10
ORDER BY avg_rating DESC, num_ratings DESC LIMIT 10;

-- Q3: MapReduce output with titles (should match Q2)
SELECT m.title, s.num_ratings, s.avg_rating
FROM mr_movie_stats s JOIN movies m ON s.movie_id = m.movie_id
WHERE s.num_ratings >= 10
ORDER BY s.avg_rating DESC, s.num_ratings DESC LIMIT 10;

-- Q4: Most-rated movies
SELECT m.title, COUNT(*) AS num_ratings, ROUND(AVG(r.rating), 2) AS avg_rating
FROM ratings r JOIN movies m ON r.movie_id = m.movie_id
GROUP BY m.title ORDER BY num_ratings DESC LIMIT 10;

-- Q5: Genre-wise average rating
SELECT genre, COUNT(*) AS num_ratings, ROUND(AVG(rating), 2) AS avg_rating
FROM (SELECT r.rating, m.genres FROM ratings r JOIN movies m ON r.movie_id = m.movie_id) t
LATERAL VIEW explode(split(t.genres, '\\|')) g AS genre
GROUP BY genre ORDER BY num_ratings DESC;

-- Q6: Rating distribution
SELECT rating, COUNT(*) AS how_many FROM ratings GROUP BY rating ORDER BY rating;

-- Q7: Live arrival trend - ratings received per minute
SELECT date_format(ts, 'yyyy-MM-dd HH:mm') AS minute, COUNT(*) AS new_ratings,
       ROUND(AVG(rating), 2) AS avg_rating
FROM ratings GROUP BY date_format(ts, 'yyyy-MM-dd HH:mm') ORDER BY minute;
