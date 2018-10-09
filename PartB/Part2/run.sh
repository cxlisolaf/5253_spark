#!/bin/bash

PYTHON_FILE=part2.py
OUTPUT_FILE=output.txt
LOCAL_OUTPUT_PATH=./

# Configure environment variables
echo ----------------------------------------------------------
echo Configure environtment variables...
if [[ $SPARK_HOME != *"usr/lib/spark"* ]]; then export SPARK_HOME=/usr/lib/spark; fi
if [[ $PYTHONPATH != *"usr/lib/spark/python"* ]]; then export PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH; fi
if [[ $PYTHONPATH != *"usr/lib/spark/python/lib/py4j-0.10.7-src.zip"* ]]; then export PYTHONPATH=$SPARK_HOME/python/lib/py4j-0.10.7-src.zip:$PYTHONPATH; fi

# Remove Existing Output
echo ----------------------------------------------------------
echo Remove existing output files...
rm ${LOCAL_OUTPUT_PATH}${OUTPUT_FILE}

# Run Spark job
echo ----------------------------------------------------------
echo Running Spark job...
spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.11:2.3.0 ${PYTHON_FILE}

## Verifying results
#echo ----------------------------------------------------------
#echo Verifying results...
#FILE_COUNT=`ls -1 ${LOCAL_OUTPUT_PATH}${OUTPUT_FILE} | wc -l`
#LINE_COUNT=`cat ${LOCAL_OUTPUT_PATH}${OUTPUT_FILE} | wc -l`
#echo Output File Count: ${FILE_COUNT}
#echo Output Line Count: ${LINE_COUNT}

echo ----------------------------------------------------------
echo Finished
echo ----------------------------------------------------------
