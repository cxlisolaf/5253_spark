# RUN USING:
# spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.11:2.3.0 test.py

from pyspark.sql import SparkSession, SQLContext

outfile = "output.txt"

# Setup metadata data
sparkMetadata = SparkSession.builder.appName("myApp") \
	.config("spark.mongodb.input.uri", "mongodb://student:student@ec2-54-173-174-196.compute-1.amazonaws.com/test.metadata") \
	.getOrCreate()
dfMetadata = sparkMetadata.read.format("com.mongodb.spark.sql").load()
sqlContextMetadata = SQLContext(sparkMetadata)
dfMetadata.registerTempTable("metadata")

# Setup item data
sparkItems = SparkSession.builder.appName("myApp") \
	.config("spark.mongodb.input.uri", "mongodb://student:student@ec2-54-173-174-196.compute-1.amazonaws.com/test.reviews-CD") \
	.getOrCreate()
dfItems = sparkItems.read.format("com.mongodb.spark.sql").load()
sqlContextItems = SQLContext(sparkItems)
dfItems.registerTempTable("items")

# Query for item review counts
items = sqlContextItems.sql("SELECT asin FROM items")
topItems = items \
	.rdd \
	.map(lambda x: (x.asin, 1)) \
	.reduceByKey(lambda x,y:x+y) \
	.filter(lambda (x,y): y >= 100) \
	.collect()

# Find review average
items = sqlContextItems.sql("SELECT asin, overall FROM items")
aTuple = (0,0)
itemAvg = items \
	.rdd \
	.aggregateByKey(aTuple, lambda a,b: (a[0] + b, a[1] + 1), lambda a,b: (a[0] + b[0], a[1] + b[1])) \
	.mapValues(lambda v: v[0]/v[1]) \
	.collect()

# Join the item count and avg dataFrames
dfLeft = sparkItems.createDataFrame(topItems, ('asin', 'num'))
dfRight = sparkItems.createDataFrame(itemAvg, ('asin', 'avg'))
dfLeft.createTempView('dfLeft')
dfRight.createTempView('dfRight')
dfItem = dfLeft \
	.join(dfRight, ['asin']) \
	.orderBy('avg', ascending=False)
dfItem.show()

# Now join the metadata
dfMetadata = sqlContextMetadata.sql("SELECT asin, title, categories FROM metadata")
dfMetadata.show()
#dfMetadata = sparkMetadata.createDataFrame(metadata, ('asin', 'title', 'categories'))
#dfMetadata.createTempView('dfMetadata')
dfJoin = dfItem \
	.join(dfMetadata, ['asin']) \
	.orderBy('avg', ascending=False)
dfJoin.show()

#topItem = dfJoin.limit(1).collect()[0]
#topMetadata = metadatas.rdd.collect()[0]
#print "TITLE: " + topMetadata.title
#print "ASIN: " + topItem.asin
#print "NUM: " + str(topItem.num)
#print "AVG: " + str(topItem.avg)

