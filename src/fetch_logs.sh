#!/bin/bash

# This script is intended to be run hourly by a cron job.
# It just fetches the last hour's appengine (webserver) logs and puts
# them in ~/kalogs.

if [ -n "$1" ]; then
    hour="$1"
else
    hour=$(date --date='last hour' '+%Y-%m-%dT%H:00:00Z')
fi
# If we turn the T and Z into spaces, 'date' can parse this.
hour_for_date=`echo "$hour" | tr TZ '  '`
hour_next=$(date --date="$hour_for_date next hour" +'%Y-%m-%dT%H:00:00Z')
hour_prev=$(date --date="$hour_for_date last hour" +'%Y-%m-%dT%H:00:00Z')

if [ -n "$2" ]; then
    log_dir="$2"
else
    log_dir="$HOME/kalogs"
fi

mkdir -p "$log_dir"

# We nest the directories, so 2012-05-07T08:00:00Z becomes
# 2012/05/07/08:00:00Z
outfile_prefix="$log_dir/`echo $hour | tr T- //`"
mkdir -p "`dirname $outfile_prefix`"

ROOT="$(dirname $0)"

export PYTHONPATH="${ROOT}:${PYTHONPATH}"   # for oauth_util/ directory

# Note that we used to run fetch_logs.py in parallel for frontends
# and backends, and due to that we used a named pipe and
# bash's "wait" to obtain the exit status of a background job.
# While we now download logs from all modules and versions together,
# we retain this technique in case we need to re-parallelize in the
# future.  (See TODO below)
tmppipe="/tmp/`basename $0`-$$"
mkfifo "$tmppipe"

# During peak hours (8am - 4pm Pacific), we generate about 20,000 log lines
# every 10 seconds. When compressed these log lines represent about 1.5 MB of
# data, which we download from GAE. The limit on response size is currently
# 10MB, which we need to stay below.
# It currently takes about 43 minutes to download 1 hour's worth of log files.
# Measured 11/5/2013
# TODO(mattfaus): Make fetch_logs.py download multiple intervals worth of logs
# concurrently, but make sure to output to the log file in-order.
"$ROOT/fetch_logs.py" -s "$hour" -e "$hour_next" \
    2> "${outfile_prefix}-status.log" \
    > "$tmppipe" &
# Store the PID to later determine the exit-code of fetch_logs.
pid_all_modules=$!
gzip -c < "$tmppipe" > "${outfile_prefix}.log.gz" &

wait $pid_all_modules
exit_code_all_modules=$?

wait  # wait to finish reading from $tmppipe in the background
rm "$tmppipe"

[[ $exit_code_all_modules == 0 ]] || exit $exit_code_all_modules
