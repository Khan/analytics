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

day_as_path=$(date --date=${day} '+%Y/%m/%d')

day_before=$(date --date=${day}-1day '+%Y-%m-%d')
day_after=$(date --date=${day}+1day '+%Y-%m-%d')

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
/usr/local/bin/s3cmd sync ~/kalogs/${day_as_path}/ \
  s3://ka-mapreduce/rawdata/server_logs/website/${day}/ 2>&1

# Convert pbuf to json + additional daily aggregation jobs
echo "Convert pbuf to json and load into the datastore"
status=$(elastic-mapreduce --create --name "${day} GAE Upload" \
  --num-instances 3 --master-instance-type m1.small \
  --slave-instance-type c1.medium \
  --json ${current_dir}/load_gae_to_hive.json \
  --param "<dt>=${day}" 2>&1 )
echo "$status"

jobid=$(echo "$status" | awk '{print $4}')
${current_dir}/../src/monitor_jobflow.py $jobid &

# UserData update
echo "Updating the UserData"
status=$(elastic-mapreduce --create --name "${day} UserDataP" \
  --num-instances 3 --master-instance-type m1.small \
  --slave-instance-type m1.large \
  --json ${current_dir}/userdata.json \
  --param "<start_dt>=${day_before}" \
  --param "<end_dt>=${day}" 2>&1 )
echo "$status"

jobid=$(echo "$status" | awk '{print $4}')
${current_dir}/../src/monitor_jobflow.py $jobid &

echo "Convert raw logs to TSV request logs"
status=$(elastic-mapreduce --create --name "${day} Request Logs Upload" \
  --num-instance 3 --master-instance-type m1.small \
  --slave-instance-type m1.large \
  --json ${current_dir}/load_request_logs_to_hive.json \
  --param "<dt>=${day}" 2>&1 )
echo "$status"

jobid=$(echo "$status" | awk '{print $4}')
${current_dir}/../src/monitor_jobflow.py $jobid &

# Daily reports 
echo "Generating daily reports"
${current_dir}/../src/report_generator.py \
  -c ${current_dir}/../cfg/daily_report.json \
  "<day>=${day}" "<day_before>=${day_before}" "<day_after>=${day_after}" 2>&1

