#!/bin/bash

# Starts up Amazon Elastic Mapreduce to run a Hive job to fill a day's worth of
# data (joined from problem logs and stack logs) for the topic_attempts table.
# This script can be ran manually or daily by a cron job.
# Arguments:
#   $1 = date to load, in format YYYY-MM-DD. Defaults to yesterday.

DATE_FORMAT='+%Y-%m-%d'

if [ -n "$1" ]; then
  day="$1"
else
  # Get yesterday's date to work in both BSD (Mac) and GNU coreutils (Linux)
  if date --version >/dev/null 2>&1 ; then
    day=$(date --date="yesterday" $DATE_FORMAT)
  else
    day=$(date -v-1d $DATE_FORMAT)
  fi
fi

# TODO(david): Use spot instances to save money
elastic-mapreduce --create \
  --name "Load topic_attempts table" \
  --log-uri "s3://ka-mapreduce/logs/" \
  --num-instances 3 \
  --master-instance-type m1.small \
  --slave-instance-type c1.medium \
  --hive-versions 0.11.0 \
  --hive-script --arg "s3://ka-mapreduce/code/hive/insert_topic_attempts.q" \
  --args -i,"s3://ka-mapreduce/code/hive/ka_hive_init.q" \
  --args -d,dt="$day" \
  --args -d,INPATH="s3://ka-mapreduce/entity_store" \
  --args -d,OUTPATH="s3://ka-mapreduce/tmp/"
