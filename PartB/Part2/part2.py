# RUN USING:
# spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.11:2.3.0 part2.py

import sys
from operator import add
import string
import re
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import regexp_replace, trim, col, lower


def string_clean(text):

	lowerCase = text.encode('utf-8').lower()
	filterword =re.sub(r'[^a-zA-Z ]','',lowerCase)
	return filterword


if __name__ == "__main__":

	# Load stopwords
	stopwords = []
	with open("./stopword.txt") as f:
		stopwords = [line.strip() for line in f]


	# Setup item data
	sparkItems = SparkSession.builder.appName("myApp") \
		.config("spark.mongodb.input.uri", "mongodb://student:student@ec2-54-210-44-189.compute-1.amazonaws.com/test.reviews") \
		.getOrCreate()
	dfItems = sparkItems.read.format("com.mongodb.spark.sql").load()
	sqlContextItems = SQLContext(sparkItems)
	dfItems.registerTempTable("items")

	# Query for item review counts, filter for items with 100 or more reviews
	reviews = sqlContextItems.sql("SELECT reviewText, overall FROM items")

	# Filter to a dataFrame for each bucket
	bucketOne = reviews.filter(reviews['overall'] == '1.0')
	bucketTwo = reviews.filter(reviews['overall'] == '2.0')
	bucketThree = reviews.filter(reviews['overall'] == '3.0')
	bucketFour = reviews.filter(reviews['overall'] == '4.0')
	bucketFive = reviews.filter(reviews['overall'] == '5.0')

	# Process BucketOne
	lines = bucketOne.rdd.map(lambda r: r[0])
	words = lines.flatMap(lambda x: string_clean(x).split(' '))
	filterwords = words.filter(lambda x: x not in stopwords)
	filterwords = filterwords.filter(lambda x: x != "")
	wordTuples = filterwords.map(lambda x: (x, 1))
	wordCountTuple = wordTuples.reduceByKey(lambda x,y:x+y).sortByKey()
	sortedWordCount = wordCountTuple.top(500,key=lambda(x,y):y)

	#Output BucketOne results
	with open('output.txt','w') as f:
		for (word, count) in sortedWordCount:
			f.write(str(word)+'\t'+str(count)+'\n')
