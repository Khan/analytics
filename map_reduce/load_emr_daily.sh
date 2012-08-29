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
archive_dir="$HOME/kabackup/daily_new"

# Bulk download small factual tables.
# TODO(yunfang): Revisit if this is the best place to do the download
${current_dir}/../src/bulk_download.py \
  -c ${current_dir}/../cfg/bulk_download.json -d $day 2>&1

# TODO(benkomalo): make command line flags consistent. "-d" is used for dir
# in gae_download.py and bingo_download.py but day in bulk_download :(
${current_dir}/../src/bingo_download.py -d ${archive_dir}


# Upload to s3.
echo "Upload to S3"
/usr/local/bin/s3cmd sync ${archive_dir}/${day}/ \
  s3://ka-mapreduce/rawdata/${day}/ 2>&1
/usr/local/bin/s3cmd sync ~/kabackup/bulkdownload/${day}/ \
  s3://ka-mapreduce/rawdata/bulk/${day}/ 2>&1

# Convert pbuf to json + additional daily aggregation jobs
echo "Convert pbuf to json and load into the datastore"
elastic-mapreduce --create --name "${day} GAE Upload" \
  --num-instances 3 --master-instance-type m1.small \
  --slave-instance-type c1.medium \
  --json ${current_dir}/load_gae_to_hive.json \
  --param "<dt>=${day}" 2>&1


# UserData update
echo "Updating the UserData"
elastic-mapreduce --create --name "${day} UserDataP" \
  --num-instances 3 --master-instance-type m1.small \
  --slave-instance-type m1.large \
  --json ${current_dir}/userdata.json \
  --param "<start_dt>=${day_before}" \
  --param "<end_dt>=${day}" 2>&1


# Daily reports 
echo "Generating daily reports"
${current_dir}/../src/report_generator.py \
  -c ${current_dir}/../cfg/daily_report.json "<dt>=${day}" 2>&1
