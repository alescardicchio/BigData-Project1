DROP TABLE reviews;
DROP TABLE result;
DROP TABLE favourite_products;
DROP TABLE users_pair_count;
DROP TABLE users_pair;
DROP TABLE users_pair_atleast_3_products;


CREATE TABLE reviews (product_id STRING, user_id STRING, profile_name STRING, helpfulness_numerator INT, helpfulness_denumerator INT, score INT, time STRING, summary STRING, text STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
tblproperties("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH 'Desktop/bigdata_project/dataset/Reviews8192.csv'
OVERWRITE INTO TABLE reviews;


CREATE TABLE favourite_products AS
SELECT user_id, product_id
FROM reviews
WHERE score >= 4;

CREATE TABLE users_pair AS
SELECT d1.user_id AS user1, d2.user_id AS user2, d1.product_id AS product_id
FROM
    (SELECT DISTINCT user_id, product_id FROM favourite_products) AS d1
    LEFT JOIN
    (SELECT DISTINCT user_id, product_id FROM favourite_products) AS d2
    ON (d1.product_id = d2.product_id)
WHERE d1.user_id < d2.user_id;

CREATE TABLE users_pair_atleast_3_products AS
SELECT user1, user2, COUNT(product_id) AS common_products
FROM users_pair
GROUP BY user1, user2
HAVING common_products >= 3;


CREATE TABLE result AS
SELECT user1, user2, product_id
FROM users_pair
WHERE EXISTS (
    SELECT user1, user2
    FROM users_pair_atleast_3_products
    WHERE (users_pair.user1 = users_pair_atleast_3_products.user1 AND users_pair.user2 = users_pair_atleast_3_products.user2))
ORDER BY user1;



DROP TABLE reviews;
DROP TABLE favourite_products;
DROP TABLE users_pair_count;
DROP TABLE users_pair;	
DROP TABLE users_pair_atleast_3_products;