#!/bin/bash

# This script is intended to be run hourly by a cron job.  We could
# run it more often if we want, but the less often we run it, the more
# likely we'll have to deal with the fact we need to fetch logs that
# were made from a app-version other than the current app-version (KA
# changes the app-version as part of the deploy process).  In theory
# we should handle that case ok, but in case we don't, more frequent
# fetches minimize the data loss due to app-version changes.

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
backend_outfile_prefix="`echo $outfile_prefix | sed 's,[^/]*$,backends-&,'`"
mkdir -p "`dirname $outfile_prefix`"

ROOT="$(dirname $0)"

export PYTHONPATH="${ROOT}:${PYTHONPATH}"   # for oauth_util/ directory

# We look in the status-log from the previous hour to figure out what
# app-version (such as 0515-ae96fc55243b) was current then.  We tell
# fetch_logs.py to pass through that app-version if we can't find logs
# associated with the now-current app-version.  This deals with the
# case where we deploy and flip the switch to change the default
# app-version, and later try to fetch logs from before the
# switch-flip.  By default, such a fetch will return no results,
# because unless you tell it otherwise, appengine only returns logs
# associated with the currently active app-version.  Looking at the
# previous fetches will tell us what version was current when the
# previous logs were generated, and we can tell that to appengine.
prev_statusfile="$log_dir/`echo $hour_prev | tr T- //`-status.log"

# Running fetch_logs.py serially for FEs and BEs takes longer than an
# hour and this script is killed by a timeout in the crontab. So we want
# to fetch FE and BE logs in parallel, but PIPESTATUS is only available
# for the most recent foreground pipeline. We use a named pipe and
# bash's "wait" to obtain the exit status of a background job.
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
pid_frontends=$!
gzip -c < "$tmppipe" > "${outfile_prefix}.log.gz" &

# Backends do not generate too much log data, so we can set the interval
# fairly high. It currently takes about 8 minutes to download 1 hour's worth
# of log files.
# Measured 11/5/2013
# TODO(jace) fetch_logs of backends became broken when we converted to
# modules in the beginning of January 2014.  So I have temporarily
# commented out the code to download them here.  We need to research how
# exactly the GAE logservice API is meant to work in the context of
# modules... Ie., how do we download logs for modules which are not the
# default?
#"$ROOT/fetch_logs.py" --backend -s "$hour" -e "$hour_next" -i 60 \
#    2> "${backend_outfile_prefix}-status.log" \
#    | gzip -c > "${backend_outfile_prefix}.log.gz"
## Use a bash-ism to return the exit-code of fetch_logs (not gzip).
#exit_code_backends=${PIPESTATUS[0]}

wait $pid_frontends
exit_code_frontends=$?

wait  # wait to finish reading from $tmppipe in the background
rm "$tmppipe"

# TODO: don't conflate failure of FE and BE fetching. Until then, check
# *-status.log for more info about which failed.
[[ $exit_code_frontends == 0 ]] || exit $exit_code_frontends
