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

# Calculate the first and last day of the current month for monthly rollup scripts
# Note that these calculations are based on yesterday, which is done collecting data
today_day=$(date +%d)
days_to_subtract=$((${today_day}-1))
month=$(date --date=${day}-1day '+%Y-%m')

if [ "${days_to_subtract}" = "0" ]; then
    # If today is the first day of a new month, we want last month
    month_first_day=$(date --date "-1 month" '+%Y-%m-%d')
    month_last_day=$(date --date "-1 day" '+%Y-%m-%d')
else
    # Else, we can just do the math on today since it works the same as yesterday
    month_first_day=$(date --date "-${days_to_subtract} days" '+%Y-%m-%d')
    month_last_day=$(date --date "+1 month -${today_day} days" '+%Y-%m-%d')
fi

current_dir=`dirname $0`
archive_dir="$HOME/kabackup/daily_new"

LOG_URI="s3://ka-mapreduce/logs/"

# Before taking off, check that gae_download has finished by counting tokens.
# If all 24 tokens are present, clear the tokens directory.
token_count=$(find ${archive_dir}/${day}/tokens -name "token*.txt" | wc -l)
while [ ${token_count} -lt 24 ]; do
    echo "Waiting for gae_download.py to finish"
    sleep 60
    token_count=$(find ${archive_dir}/${day}/tokens -name "token*.txt" | wc -l)
done

# Bulk download small factual tables.
# TODO(yunfang): Revisit if this is the best place to do the download
${current_dir}/../src/bulk_download.py \
  -c ${current_dir}/../cfg/bulk_download.json -d $day 2>&1

# TODO(benkomalo): make command line flags consistent. "-d" is used for dir
# in gae_download.py and bingo_download.py but day in bulk_download :(
${current_dir}/../src/bingo_download.py -d ${archive_dir} -s $day


# Upload to s3.
echo "Upload to S3"
/usr/local/bin/s3cmd sync ${archive_dir}/${day}/ \
  s3://ka-mapreduce/rawdata/${day}/ 2>&1
/usr/local/bin/s3cmd sync ~/kabackup/bulkdownload/${day}/ \
  s3://ka-mapreduce/rawdata/bulk/${day}/ 2>&1


# TODO(jace) As of 4/13/2014, the rest of this script has been
# broken for over a month.  I am inserting an early exit, until
# I can determine whather (or how much) of this pipeline can be killed
# with fire.
exit 0

# -------------------------------------------------------------------
# NOTE:  the rest of this script will not execute due to above exit()
# -------------------------------------------------------------------

# Convert pbuf to json + additional daily aggregation jobs
echo "Convert pbuf to json and load into the datastore"
status=$(elastic-mapreduce --create --name "${day} GAE Upload" \
  --num-instances 3 --master-instance-type m2.xlarge \
  --slave-instance-type m2.xlarge \
  --log-uri "$LOG_URI" \
  --json ${current_dir}/load_gae_to_hive.json \
  --param "<dt>=${day}" 2>&1 )
echo "$status"

jobid=$(echo "$status" | awk '{print $4}')
${current_dir}/../src/monitor_jobflow.py $jobid &

# Update *Incr and *P tables, for example UserDataIncr and UserDataP
echo "Updating the UserData"
status=$(elastic-mapreduce --create --name "${day} UserDataP" \
  --num-instances 5 --master-instance-type m2.xlarge \
  --slave-instance-type m2.xlarge \
  --log-uri "$LOG_URI" \
  --json ${current_dir}/userdata.json \
  --param "<start_dt>=${day_before}" \
  --param "<end_dt>=${day}" 2>&1 )
echo "$status"

jobid=$(echo "$status" | awk '{print $4}')
${current_dir}/../src/monitor_jobflow.py $jobid &

jobid=$(echo "$status" | awk '{print $4}')
${current_dir}/../src/monitor_jobflow.py $jobid &

# Daily reports
echo "Generating daily reports"
${current_dir}/../src/report_generator.py \
  -c ${current_dir}/../cfg/daily_report.json \
  "<day>=${day}" "<day_before>=${day_before}" "<day_after>=${day_after}" \
  "<month_first_day>=${month_first_day}" "<month_last_day>=${month_last_day}" \
  "<month>=${month}" 2>&1
