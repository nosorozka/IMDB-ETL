USE ROLE TRAINING_ROLE;
CREATE WAREHOUSE IF NOT EXISTS HYENA_WH;
USE WAREHOUSE HYENA_WH;
USE IMDB_DB.PUBLIC;
USE SCHEMA IMDB_DB.staging;
-- SHOW TABLES;

-- Graf 1: Pomer mužov a žien vo filmovom priemysle
SELECT sex, COUNT(*)FROM dim_names WHERE sex != 'unknown'
GROUP BY sex;

-- Graf 2: 10 krajin ktoré sa najčastejšie podieľajú na natáčaní filmov
SELECT country_name, COUNT(*) AS count FROM movie_country_mapping
GROUP BY country_name
ORDER BY count DESC
LIMIT 10; 

-- Graf 3: Najpopulárnejšie dabingové jazyky
SELECT language_name, COUNT(*) AS count FROM movie_language_mapping
GROUP BY language_name
ORDER BY count DESC
LIMIT 10; 

-- Graf 4: 5 najobľúbenejších filmov podľa celkových zbierok
SELECT DISTINCT id, title, WORLDWIDE_GROSS_INCOME  FROM FAKT_MOVIE
WHERE WORLDWIDE_GROSS_INCOME != 'NULL'
ORDER BY CAST(REGEXP_REPLACE(WORLDWIDE_GROSS_INCOME, '[^0-9]', '') AS DECIMAL) DESC
LIMIT 5;

-- Graf 5: 5 najobľúbenejších filmov podľa priemerného hodnotenia
SELECT * FROM FAKT_MOVIE
WHERE WORLDWIDE_GROSS_INCOME != 'NULL'
ORDER BY rating DESC
LIMIT 5;

-- Graf 6: či závisí priemerné hodnotenie od dĺžky filmu
SELECT DURATION, COUNT(*), round(avg(rating),2) FROM FAKT_MOVIE
GROUP BY DURATION
ORDER BY DURATION ASC;

-- Graf 7: Ktorý režisér nakrútil najviac filmov
SELECT n.id, n.name, COUNT(mnm.movie_id) AS movie_count
FROM dim_names n
JOIN movie_names_mapping mnm ON n.id = mnm.names_id
JOIN fakt_movie f ON mnm.movie_id = f.id
WHERE mnm.director = TRUE
GROUP BY n.id, n.name 
ORDER BY movie_count DESC;


SELECT distinct id FROM dim_names;



