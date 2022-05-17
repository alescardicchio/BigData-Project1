#!/usr/bin/env python3
"""spark application"""

import argparse
from pyspark.sql import SparkSession
import itertools

parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
parser.add_argument("--output_path", type=str, help="Output folder path")

args = parser.parse_args()
input_filepath, output_filepath = args.input_path, args.output_path

spark = SparkSession \
    .builder \
    .appName("Job3 Spark") \
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

def listFunction(v):
	tmp = list(v)
	tmp.sort()
	return list(itertools.combinations(tmp, 2))

def reverseMapping(x):
	output = []
	for couple in x[1]:
		output.append((couple, x[0]))
	return output

raw_data = spark.sparkContext.textFile(input_filepath).filter(lambda line: not line.startswith("ProductId"))

data = raw_data.filter(lambda line: int(line.split(",")[SCORE]) >= 4).map(lambda line: (line.split(",")[PRODUCT], line.split(",")[USER])).groupByKey().mapValues(listFunction)

reverseMapped = data.flatMap(reverseMapping).groupByKey().filter(lambda elem: len(elem[1])>=3).mapValues(list).sortByKey()

reverseMapped.saveAsTextFile(output_filepath)