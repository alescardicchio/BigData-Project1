#!/usr/bin/env python3
"""spark application"""

import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from collections import Counter

parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
parser.add_argument("--output_path", type=str, help="Output folder path")

args = parser.parse_args()
input_filepath, output_filepath = args.input_path, args.output_path

spark = SparkSession \
    .builder \
    .appName("Job1 Spark") \
    .config("spark.executor.memory", "500m") \
    .getOrCreate()

amazon_food_reviews_RDD = spark.sparkContext.textFile(input_filepath).cache()

PRODUCT = 0
USER = 1
PROFILE_NAME = 2
HELPFULNESS_NUMERATOR = 3
HELPFULNESS_DENOMINATOR = 4
SCORE = 5
TIME = 6
SUMMARY = 7
TEXT = 8

def word_count(list):
    word2count = dict()
    
    for word in list:
        if word in word2count:
            word2count[word] += 1
        else:
            word2count[word] = 1
    
    return word2count

reviews_no_header_RDD = amazon_food_reviews_RDD.filter(lambda line: not line.startswith("ProductId"))

splitted_input_RDD = reviews_no_header_RDD.map(lambda line: line.strip().split(','))

year2text_RDD = splitted_input_RDD.map(lambda line: (int(datetime.fromtimestamp(int(line[TIME])).strftime('%Y')), line[TEXT]))

year2texts_byKey_RDD = year2text_RDD.reduceByKey(lambda x, y: x + y)

year2words_byKey_RDD = year2texts_byKey_RDD.map(lambda line: (line[0], line[1].strip().split(" ")))

year2words_byKey_word2count_RDD = year2words_byKey_RDD.map(lambda x: (x[0], word_count(x[1])))

year2words_byKey_word2count_orderByWordsFrequency_RDD = year2words_byKey_word2count_RDD.map(lambda x: (x[0], sorted(x[1].items(), key=lambda y: y[1], reverse=True)))

year2words_byKey_word2count_orderByWordsFrequency_first10_RDD = year2words_byKey_word2count_orderByWordsFrequency_RDD.map(lambda x: (x[0], x[1][:10]))

year2words_byKey_word2count_orderByWordsFrequency_first10_RDD.saveAsTextFile(output_filepath)


