DROP TABLE IF EXISTS reviews;
DROP TABLE IF EXISTS year_text;
DROP TABLE IF EXISTS year_explodedText;
DROP TABLE IF EXISTS year_word_count;
DROP TABLE IF EXISTS job1_result;

CREATE TABLE reviews (product_id STRING, user_id STRING, profile_name STRING, helpfulness_numerator INT, helpfulness_denumerator INT, score INT, time INT, summary STRING, text STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
tblproperties("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH 'Desktop/bigdata_project/dataset/Reviews_Clean.csv'
OVERWRITE INTO TABLE reviews;

CREATE TABLE year_text AS
SELECT year(from_unixtime((time))) as year, text
FROM reviews;

CREATE TABLE year_explodedText AS
SELECT year, word
FROM year_text LATERAL VIEW explode(split(text, ' ')) single_word AS word;

CREATE TABLE year_word_count AS 
SELECT year, word, COUNT(word) AS word_occurrences
FROM year_explodedText
GROUP BY year, word;

CREATE TABLE job1_result AS
SELECT year, word, word_occurrences
FROM 
    (
      SELECT 
          *,
          ROW_NUMBER() OVER (PARTITION BY year ORDER BY word_occurrences DESC) AS row_number
      FROM 
          year_word_count
    ) t
WHERE 
    row_number <= 10
ORDER BY year;


DROP TABLE reviews;
DROP TABLE year_text;
DROP TABLE year_explodedText;
DROP TABLE year_word_count;