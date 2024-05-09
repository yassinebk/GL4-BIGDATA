#! /bin/bash

hdfs dfs -rm -R /input /output
hdfs dfs -mkdir /input 
hdfs dfs -put /input /datasets/*csv

BATCH_DIRECTORY=spark/batch

python3 "$BATCH_DIRECTORY/job-titles.py"
python3 "$BATCH_DIRECTORY/locations.py"
python3 "$BATCH_DIRECTORY/most-repeated-words.py"
python3 "$BATCH_DIRECTORY/job-per-company.py"