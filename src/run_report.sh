#!/bin/bash
# This script take in an argument to know the duration and then generates
# command to run report_generator.py

# Get duration
if [ "$1" = "month" ]; then 
  end_dt=$(date "+%Y-%m-01")
  start_dt=$(date --date=${end_dt}-1month "+%Y-%m-01")
elif [ "$1" = "week" ]; then
  end_dt=$(date "+%Y-%m-%d")
  start_dt=$(date --date=${end_dt}-1week "+%Y-%m-%d")
else
  echo "not a right duration '$1'"
  exit 1
fi
end_dt_inclusive=$(date --date=${end_dt}-1day "+%Y-%m-%d")

# Generate and run the command
current_dir=`dirname $0`
command="${current_dir}/../src/report_generator.py -c ${current_dir}/../cfg/regular_report.json <duration>=$1 <start_dt>=${start_dt} <end_dt>=${end_dt} <end_dt_inclusive>=${end_dt_inclusive}"
echo $command
$command 
