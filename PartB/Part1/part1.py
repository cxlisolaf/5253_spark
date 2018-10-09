# RUN USING:
# spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.11:2.3.0 test.py

from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import array_contains

outfile = "output.txt"

# Setup metadata data
sparkMetadata = SparkSession.builder.appName("myApp") \
	.config("spark.mongodb.input.uri", "mongodb://student:student@ec2-54-210-44-189.compute-1.amazonaws.com/test.metadata") \
	.getOrCreate()
dfMetadata = sparkMetadata.read.format("com.mongodb.spark.sql").load()
sqlContextMetadata = SQLContext(sparkMetadata)
dfMetadata.registerTempTable("metadata")

# Setup item data
sparkItems = SparkSession.builder.appName("myApp") \
	.config("spark.mongodb.input.uri", "mongodb://student:student@ec2-54-210-44-189.compute-1.amazonaws.com/test.reviews") \
	.getOrCreate()
dfItems = sparkItems.read.format("com.mongodb.spark.sql").load()
sqlContextItems = SQLContext(sparkItems)
dfItems.registerTempTable("items")

# Query for item review counts, filter for items with 100 or more reviews
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

# Join the item count and avg dataFrames and sort by average review descending
dfLeft = sparkItems.createDataFrame(topItems, ('asin', 'num'))
dfRight = sparkItems.createDataFrame(itemAvg, ('asin', 'avg'))
dfLeft.createTempView('dfLeft')
dfRight.createTempView('dfRight')
dfItem = dfLeft \
	.join(dfRight, ['asin']) \
	.orderBy('avg', ascending=False)

# Now join the metadata
dfMetadata = sqlContextMetadata.sql("SELECT asin, title, categories FROM metadata")
dfMetadata.show()
dfJoin = dfItem \
	.join(dfMetadata, ['asin']) \
	.orderBy('avg', ascending=False)

# Filter to a dataFrame for each category
df_CDs = dfJoin.filter(array_contains(dfJoin['categories'], 'CDs & Vinyl'))
df_movies = dfJoin.filter(array_contains(dfJoin['categories'], 'Movies & TV'))
df_videoGames = dfJoin.filter(array_contains(dfJoin['categories'], 'Video Games'))
df_Toys = dfJoin.filter(array_contains(dfJoin['categories'], 'Toys & Games'))

# Get the top record for each category
topCD = df_CDs.limit(1).collect()
topMovie = df_movies.limit(1).collect()
topVideoGame = df_videoGames.limit(1).collect()
topToy = df_Toys.limit(1).collect()

# Output results
file = open(outfile, "w")
if len(topCD) > 0 and hasattr(topCD[0], 'num'):
    file.write("CDs & Vinyl"+'\t'+str(topCD[0].title)+'\t'+str(topCD[0].num)+'\t'+str(topCD[0].avg)+'\n')
if len(topMovie) > 0 and hasattr(topMovie[0], 'num'):
    file.write("Movies & TV"+'\t'+str(topMovie[0].title)+'\t'+str(topMovie[0].num)+'\t'+str(topMovie[0].avg)+'\n')
if len(topToy) > 0 and hasattr(topToy[0], 'num'):
    file.write("Toys & Games"+'\t'+str(topToy[0].title)+'\t'+str(topToy[0].num)+'\t'+str(topToy[0].avg)+'\n') 
if len(topVideoGame) > 0 and hasattr(topVideoGame[0], 'num'):
    file.write("Video Games"+'\t'+str(topVideoGame[0].title)+'\t'+str(topVideoGame[0].num)+'\t'+str(topVideoGame[0].avg)+'\n')
file.close()
