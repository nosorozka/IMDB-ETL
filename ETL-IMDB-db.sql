USE ROLE TRAINING_ROLE;
CREATE WAREHOUSE IF NOT EXISTS HYENA_WH;
USE WAREHOUSE HYENA_WH;

-- Vytvorenie databázy
CREATE DATABASE IMDB_DB;
USE IMDB_DB.PUBLIC;

-- Vytvorenie schémy pre staging tabuľky
CREATE SCHEMA IMDB_DB.staging;

USE SCHEMA IMDB_DB.staging;

-- Vytvorenie tabuľky movie (staging)
CREATE TABLE movie_staging (
    id VARCHAR(10) PRIMARY KEY,
    title VARCHAR(200),
    year INT,
    date_published DATE,
    duration INT,
    country VARCHAR(250),
    worldwide_gross_income VARCHAR(30),
    languages VARCHAR(200),
    production_company VARCHAR(200)
);

-- Vytvorenie tabuľky genre (staging)
CREATE TABLE genre_staging (
    movie_id VARCHAR(10),
    genre VARCHAR(20) PRIMARY KEY,
    FOREIGN KEY (movie_id) REFERENCES movie_staging(id)  
);

-- Vytvorenie tabuľky rating (staging)
CREATE TABLE rating_staging (
    movie_id VARCHAR(10),
    FOREIGN KEY (movie_id) REFERENCES movie_staging(id),
    avg_rating DECIMAL(3,1),
    total_votes INT,
    median_rating INT
);

-- Vytvorenie tabuľky names (staging)

-- zakážeme obmedzenie NULL stĺpcov, aby sme sa vyhli chybám.
ALTER TABLE names_staging ALTER COLUMN height DROP NOT NULL;
ALTER TABLE names_staging ALTER COLUMN date_of_birth DROP NOT NULL;
DESCRIBE TABLE names_staging;

CREATE TABLE names_staging (
    id VARCHAR(10) PRIMARY KEY,
    name VARCHAR(100),
    height INT,
    date_of_birth DATE,
    known_for_movies VARCHAR(100)
);

-- Vytvorenie tabuľky director_mapping (staging)
CREATE TABLE director_mapping_staging (
    movie_id VARCHAR(10),
    name_id VARCHAR(10),
    FOREIGN KEY (movie_id) REFERENCES movie_staging(id),
    FOREIGN KEY (name_id) REFERENCES names_staging(id)
);

-- Vytvorenie tabuľky role_mapping (staging)
CREATE TABLE role_mapping_staging (
    movie_id VARCHAR(10),
    name_id VARCHAR(10),
    category VARCHAR(10),
    FOREIGN KEY (movie_id) REFERENCES movie_staging(id),
    FOREIGN KEY (name_id) REFERENCES names_staging(id)
);

 SHOW TABLES;

-- Vytvorenie my_stage pre .csv súbory
CREATE OR REPLACE STAGE my_stage;


COPY INTO genre_staging
FROM @my_stage/genre.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO movie_staging
FROM @my_stage/movie.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO role_mapping_staging
FROM @my_stage/role_mapping.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO rating_staging
FROM @my_stage/ratings.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO director_mapping_staging
FROM @my_stage/director_mapping.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO names_staging
FROM @my_stage/names.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
VALIDATION_MODE = 'RETURN_ERRORS';

-- vytvorme nový formát nahrávania, aby sme správne nahrali riadky obsahujúce NULL

CREATE OR REPLACE FILE FORMAT csv_format
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  NULL_IF = ('NULL', '');
  
COPY INTO names_staging
FROM @my_stage/names.csv
FILE_FORMAT = csv_format;
VALIDATION_MODE = 'RETURN_ERRORS';
  
-- SELECT * FROM names_staging;






------------------------------------------------------------------------------------------------------------------------------------------







--- ELT - (T)ransform

-- dim_ratings
CREATE TABLE rating_with_id AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY movie_id) AS rating_id,  -- Poradové číslo pre každý záznam
    movie_id, 
    avg_rating,
    total_votes,
    median_rating
FROM rating_staging;

-- dim_genre
CREATE OR REPLACE TABLE dim_genre AS
SELECT
    movie_id,
    genre AS genre_name 
FROM genre_staging;

-- dim_languages
CREATE OR REPLACE TABLE dim_languages AS
SELECT DISTINCT
    TRIM(value) AS language_name
FROM movie_staging,
LATERAL FLATTEN(INPUT => SPLIT(languages, ',')) AS language_split;

-- dim_country
CREATE OR REPLACE TABLE dim_country AS
SELECT DISTINCT
    TRIM(value) AS country_name
FROM movie_staging,
LATERAL FLATTEN(INPUT => SPLIT(country, ',')) AS country_split;

-- dim_names
CREATE OR REPLACE TABLE dim_names AS
SELECT 
    n.id,
    n.name,
    n.height,
    n.date_of_birth,
    n.known_for_movies,
    CASE
        WHEN r.category = 'actor' THEN 'actor'
        WHEN r.category = 'actress' THEN 'actress'
        ELSE 'unknown'
    END AS role,
    CASE
        WHEN r.category = 'actor' THEN 'male'
        WHEN r.category = 'actress' THEN 'female'
        ELSE 'unknown' 
    END AS sex
FROM names_staging n
JOIN role_mapping_staging r ON n.id = r.name_id;

-- FACT_MOVIE
CREATE OR REPLACE TABLE fakt_movie AS 
SELECT id,                      -- Unikátne ID filmu
    m.title,                    -- Nazov
    m.year,                     
    m.date_published,
    m.duration,
    m.worldwide_gross_income,
    m.production_company,
    r.avg_rating AS rating,     -- Prepojenie s dimenziou retingu
    g.genre_name AS genre       -- Prepojenie s dimenziou ganru
FROM movie_staging m
JOIN rating_with_id r ON m.id = r.movie_id
JOIN dim_genre g ON m.id = g.movie_id;

-- movie_language_mapping
CREATE OR REPLACE TABLE movie_language_mapping AS 
SELECT 
    m.id AS movie_id,
    TRIM(f.value::STRING) AS language_name
FROM movie_staging m,
     LATERAL FLATTEN(input => SPLIT(m.languages, ',')) f;

-- movie_country_mapping      
CREATE OR REPLACE TABLE movie_country_mapping AS 
SELECT 
    m.id AS movie_id,
    TRIM(f.value::STRING) AS country_name
FROM movie_staging m,
     LATERAL FLATTEN(input => SPLIT(m.country, ',')) f;

-- movie_names_mapping 
CREATE OR REPLACE TABLE movie_names_mapping (
    names_id VARCHAR(10),     
    movie_id VARCHAR(10),     
    director BOOLEAN  
);

INSERT INTO movie_names_mapping (names_id, movie_id, director)
SELECT 
    name_id AS names_id, 
    movie_id,
    TRUE AS director 
FROM director_mapping_staging;

INSERT INTO movie_names_mapping (names_id, movie_id, director)
SELECT 
    name_id AS names_id, 
    movie_id,
    FALSE AS director 
FROM role_mapping_staging;

-- SELECT * FROM movie_names_mapping;

-- DROP stagging tables
DROP TABLE IF EXISTS genre_staging;
DROP TABLE IF EXISTS movie_staging;
DROP TABLE IF EXISTS role_mapping_staging;
DROP TABLE IF EXISTS rating_staging;
DROP TABLE IF EXISTS director_mapping_staging;

