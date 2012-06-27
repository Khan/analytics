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

day_before=$(date --date=${day}-1day '+%Y-%m-%d')

current_dir=`dirname $0`

# Bulk download small factual tables 
# TODO(yunfang): Revisit if this is the best place to do the download
${curren_dir}/../src/bulk_download.py \
  -c ${curren_dir}/../cfg/bulk_download.json -d $day 2>&1


# Upload to s3.
echo "Upload to S3"
/usr/local/bin/s3cmd sync ~/kabackup/daily_new/${day}/ \
  s3://ka-mapreduce/rawdata/${day}/ 2>&1
/usr/local/bin/s3cmd sync ~/kabackup/bulkdownload/${day}/ \
  s3://ka-mapreduce/rawdata/bulk/${day}/ 2>&1

# Convert pbuf to json
echo "Convert pbuf to json and load into the datastore"
elastic-mapreduce --create --name "${day} GAE Upload" \
  --num-instances 3 --master-instance-type m1.small \
  --slave-instance-type c1.medium \
  --json `dirname $0`/load_gae_to_hive.json \
  --param "<dt>=${day}" 2>&1


# UserData update
echo "Updating the UserData"
elastic-mapreduce --create --name "${day} UserDataP" \
  --num-instances 3 --master-instance-type m1.small \
  --slave-instance-type m1.large \
  --json `dirname $0`/userdata.json \
  --param "<start_dt>=${day_before}" \
  --param "<end_dt>=${day}" 2>&1
