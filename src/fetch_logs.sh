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

if [ -n "$3" ]; then
    backend_log_dir="$2"
else
    backend_log_dir="$HOME/kabackendlogs"
fi

mkdir -p "$log_dir"
mkdir -p "$backend_log_dir"

# We nest the directories, so 2012-05-07T08:00:00Z becomes
# 2012/05/07/08:00:00Z
outfile_prefix="$log_dir/`echo $hour | tr T- //`"
backend_outfile_prefix="$backend_log_dir/`echo $hour | tr T- //`"
mkdir -p "`dirname $outfile_prefix`"
mkdir -p "`dirname $backend_outfile_prefix`"

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

"$ROOT/fetch_logs.py" -s "$hour" -e "$hour_next" \
    --file_for_alternate_appengine_versions="$prev_statusfile" \
    2> "${outfile_prefix}-status.log" \
    | gzip -c > "${outfile_prefix}.log.gz"

"$ROOT/fetch_logs.py" --backend -s "$hour" -e "$hour_next" \
    --file_for_alternate_appengine_versions="$prev_statusfile" \
    2> "${backend_outfile_prefix}-status.log" \
    | gzip -c > "${backend_outfile_prefix}.log.gz"

# Use a bash-ism to return the exit-code of fetch_logs (not gzip).
exit ${PIPESTATUS[0]}
