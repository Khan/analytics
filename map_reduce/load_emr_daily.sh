#!/bin/bash

# This script is intended to be run daily by a cron.
# It uploaded the downloaded GAE data to s3 and kicks off the map/reduce job
# to load the data to our entity_store

# Get day. 
if [ -n "$1" ]; then
    day="$1"
else
    day=$(date --date='yesterday' '+%Y-%m-%d')
fi 

mkdir -p ~/kalogs/load_emr

# Upload to s3. 
/usr/local/bin/s3cmd sync ~/kabackup/daily_new/${day}/ \
  s3://ka-mapreduce/rawdata/${day}/ >~/kalogs/load_emr/${day}.log  2>&1


# Convert pbuf to json
/home/analytics/emr/elastic-mapreduce --create --name "${day} GAE Upload" \
  --num-instances 3 --master-instance-type m1.small \
  --slave-instance-type c1.medium \
  --json ~/analytics/map_reduce/load_pbufs_to_hive.json \
  --param "<dt>=${day}" >>~/kalogs/load_emr/${day}.log  2>&1
