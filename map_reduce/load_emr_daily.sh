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

# Upload to s3. 
echo "Upload to S3"
/usr/local/bin/s3cmd sync ~/kabackup/daily_new/${day}/ \
  s3://ka-mapreduce/rawdata/${day}/ 2>&1


# Convert pbuf to json
echo "Convert pbuf to json and load into the datastore"
/home/analytics/emr/elastic-mapreduce --create --name "${day} GAE Upload" \
  --num-instances 3 --master-instance-type m1.small \
  --slave-instance-type c1.medium \
  --json ~/analytics/map_reduce/load_pbufs_to_hive.json \
  --param "<dt>=${day}" 2>&1
