DROP TABLE IF EXISTS reviews;
DROP TABLE IF EXISTS words;
DROP TABLE IF EXISTS favourite_products;
DROP TABLE IF EXISTS job2_result;

CREATE TABLE reviews (product_id STRING, user_id STRING, profile_name STRING, helpfulness_numerator INT, helpfulness_denumerator INT, score INT, time STRING, summary STRING, text STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
tblproperties("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH 'Desktop/bigdata_project/dataset/Reviews8192.csv'
OVERWRITE INTO TABLE reviews;

CREATE TABLE favourite_products AS
SELECT user_id, product_id, score
FROM reviews;

CREATE TABLE job2_result AS
SELECT user_id, product_id, score
FROM 
    (
      SELECT 
          *,
          ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY score DESC) AS row_number
      FROM 
          favourite_products
    ) t
WHERE 
    row_number <= 5
ORDER BY user_id;


DROP TABLE reviews;
DROP TABLE words;
DROP TABLE favourite_products;
	