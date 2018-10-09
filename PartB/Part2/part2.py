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

	reviews = sqlContextItems.sql("SELECT reviewText, overall FROM items")

	bucketList = []

	# Filter to a dataFrame for each bucket
	bucketOne = reviews.filter(reviews['overall'] == '1.0')
	bucketList.append(bucketOne)
	bucketTwo = reviews.filter(reviews['overall'] == '2.0')
	bucketList.append(bucketTwo)
	bucketThree = reviews.filter(reviews['overall'] == '3.0')
	bucketList.append(bucketThree)
	bucketFour = reviews.filter(reviews['overall'] == '4.0')
	bucketList.append(bucketFour)
	bucketFive = reviews.filter(reviews['overall'] == '5.0')
	bucketList.append(bucketFive)

	for i in range(0,5):

		filename = "output" + str(i+1) +".txt"
		bucket =  bucketList[i]
		# Process BucketOne
		lines = bucket.rdd.map(lambda r: r[0])
		words = lines.flatMap(lambda x: string_clean(x).split(' '))
		filterwords = words.filter(lambda x: x not in stopwords)
		filterwords = filterwords.filter(lambda x: x != "")
		wordTuples = filterwords.map(lambda x: (x, 1))
		wordCountTuple = wordTuples.reduceByKey(lambda x,y:x+y).sortByKey()
		sortedWordCount = wordCountTuple.top(500,key=lambda(x,y):y)

		#Output BucketOne results
		with open(filename,'w') as f:
			for (word, count) in sortedWordCount:
				f.write(str(word)+'\t'+str(count)+'\n')


