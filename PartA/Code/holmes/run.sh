#!/bin/bash

INPUT_FILE=big.txt
PYTHON_FILE=../WordCount.py
STOPWORD_FILE=stopword.txt
OUTPUT_FILE=output.txt
LOCAL_INPUT_PATH=./input
LOCAL_OUTPUT_PATH=./
LOCAL_STOPWORD_PATH=../
HDFS_INPUT_PATH=/input
HDFS_URI=$(hdfs getconf -nnRpcAddresses)

# Remove Existing Output
echo ----------------------------------------------------------
echo Remove existing output files...
rm ${LOCAL_OUTPUT_PATH}${OUTPUT_FILE}

# Create directories
echo ----------------------------------------------------------
echo Creating directories...
hdfs dfs -mkdir ${HDFS_INPUT_PATH}

# Configure environment variables
echo ----------------------------------------------------------
echo Configure environtment variables...
if [[ $SPARK_HOME != *"usr/lib/spark"* ]]; then export SPARK_HOME=/usr/lib/spark; fi
if [[ $PYTHONPATH != *"usr/lib/spark/python"* ]]; then export PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH; fi
if [[ $PYTHONPATH != *"usr/lib/spark/python/lib/py4j-0.10.7-src.zip"* ]]; then export PYTHONPATH=$SPARK_HOME/python/lib/py4j-0.10.7-src.zip:$PYTHONPATH; fi

# Move data file to hdfs
echo ----------------------------------------------------------
echo Copying input file to hdfs...
hdfs dfs -copyFromLocal ${LOCAL_INPUT_PATH}/${INPUT_FILE} ${HDFS_INPUT_PATH}

# Run Spark job
echo ----------------------------------------------------------
echo Running Spark job...
python ${PYTHON_FILE} hdfs://${HDFS_URI}${HDFS_INPUT_PATH}/${INPUT_FILE} ${LOCAL_STOPWORD_PATH}${STOPWORD_FILE}

# Cleanup hdfs files
echo ----------------------------------------------------------
echo Cleaning up hdfs...
hdfs dfs -rm -r ${HDFS_INPUT_PATH}

# Verifying results
echo ----------------------------------------------------------
echo Verifying results...
FILE_COUNT=`ls -1 ${LOCAL_OUTPUT_PATH}${OUTPUT_FILE} | wc -l`
LINE_COUNT=`cat ${LOCAL_OUTPUT_PATH}${OUTPUT_FILE} | wc -l`
echo Output File Count: ${FILE_COUNT}
echo Output Line Count: ${LINE_COUNT}

echo ----------------------------------------------------------
echo Finished
echo ----------------------------------------------------------
