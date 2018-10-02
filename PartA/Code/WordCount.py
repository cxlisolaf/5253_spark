from __future__ import print_function
import sys
from operator import add
import string
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, trim, col, lower

###
###Code is based on word count sample code from ApacheSpark.org
###
### python WordCount.py hdfs://$(hdfs getconf -nnRpcAddresses)/user/hadoop/wordcount/xxx.txt stopword.txt

def string_clean(text):

	lowerCase = text.encode('utf-8').lower()
	filterword =re.sub(r'[^a-zA-Z ]','',lowerCase)
	return filterword


if __name__ == "__main__":



    if len(sys.argv) != 3:
        print("Usage: wordcount <file1> <file2>", file=sys.stderr)
        exit(-1)
        
    spark = SparkSession\
        .builder\
        .appName("PythonWordCount")\
        .getOrCreate()

    file1 = sys.argv[1]
    file2 = sys.argv[2]
    stopwords = []
    outfile = "output.txt"

    with open(file2) as f:
    	stopwords = [line.strip() for line in f]
        
    # read file and write into new RDD instance lines
    dataset = spark.read.text(file1)

    lines = dataset.rdd.map(lambda r: r[0])

    words = lines.flatMap(lambda x: string_clean(x).split(' ')) 

    filterwords = words.filter(lambda x: x not in stopwords)
    filterwords = filterwords.filter(lambda x: x != "")

    wordTuples = filterwords.map(lambda x: (x, 1))
    wordCountTuple = wordTuples.reduceByKey(lambda x,y:x+y)

    sortedWordCount = wordCountTuple.top(2000,key=lambda(x,y):y)
                     
    with open(outfile,'w') as f:
	    for (word, count) in sortedWordCount:
	    	f.write(str(word)+'\t'+str(count)+'\n')
	        #print("%s\t%i" % (word, count))


    spark.stop()

