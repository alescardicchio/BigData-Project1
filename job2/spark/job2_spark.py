#!/usr/bin/env python3
"""spark application"""

import argparse
from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
parser.add_argument("--output_path", type=str, help="Output folder path")

args = parser.parse_args()
input_filepath, output_filepath = args.input_path, args.output_path

spark = SparkSession \
    .builder \
    .appName("Job2 Spark") \
    .config("spark.executor.memory", "500m") \
    .getOrCreate()

PRODUCT = 0
USER = 1
PROFILE_NAME = 2
HELPFULNESS_NUMERATOR = 3
HELPFULNESS_DENOMINATOR = 4
SCORE = 5
TIME = 6
SUMMARY = 7
TEXT = 8

amazon_food_reviews_RDD = spark.sparkContext.textFile(input_filepath).cache()

reviews_no_header_RDD = amazon_food_reviews_RDD.filter(lambda line: not line.startswith("ProductId"))

splitted_input_RDD = reviews_no_header_RDD.map(lambda line: line.strip().split(','))

user2products_RDD = splitted_input_RDD.map(lambda line: (line[USER], [(line[PRODUCT], int(line[SCORE]))]))

user2products_byKey_RDD = user2products_RDD.reduceByKey(lambda x, y: x + y)

user2products_byKey_orderedByScore_RDD = user2products_byKey_RDD.map(lambda x: (x[0], sorted(x[1], key=lambda y: y[1], reverse=True)))

user2products_byKey_orderedByScore_first5_RDD = user2products_byKey_orderedByScore_RDD.map(lambda x: (x[0], x[1][:5]))

user2products_byKey_orderedByScore_first5_orderedByUserID_RDD = user2products_byKey_orderedByScore_first5_RDD.sortByKey()


user2products_byKey_orderedByScore_first5_orderedByUserID_RDD.saveAsTextFile(output_filepath)