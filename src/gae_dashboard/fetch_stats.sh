#!/bin/sh

set -e

# Usage: fetch_stats.sh [-n] [-v]
#
# -n runs reports as "dry-runs", they will not write to the database.
# -v prints reports to stdout.

report_opts=
set -- `getopt vn "$@"`
while [ $# -gt 0 ]
do
    case "$1" in
	-n) report_opts="$report_opts -n";;
	-v) report_opts="$report_opts -v";;
	--) shift; break;;
	-*) echo "usage: $0 [-n] [-v]" >&2
	    exit 1;;
	*)  break;;
    esac
    shift
done

: ${srcdir:="${HOME}/analytics/src/gae_dashboard"}
: ${private_pw:="${HOME}/private_pw"}
: ${username:="khanbackups@gmail.com"}
: ${curl_app:="${srcdir}/gae_dashboard_curl.py"}
: ${app_id:="s~khan-academy"}

if [ ! -e "${private_pw}" ]; then
    echo "Need to put password for ${username} in ${private_pw}"
    exit 1
fi

"${srcdir}/ka_report.py" -e "${username}" -A "${app_id}" ${report_opts} \
    < "${private_pw}"

# For the dashboard, we have to fetch lots of different urls these
# days, since each of the dashboard graphs is from a different url.
# The urls send back json with chart_url data, which we then send
# to the dashboard_report script to parse.
#
# We set the time-range to 5 minutes since we run this from a cronjob
# once every 5 minutes.
# TODO(csilvers): store the last timestamp we got data from in a file,
#     instead of hard-coding '5 minutes ago'.
start_time=`date +%s -d '5 minutes ago'`
end_time=`date +%s`
{
    # We look in dashboard_report.py to get the number of charts to
    # fetch.  Ugh.
    num_charts=`env PYTHONPATH="${srcdir}" python -c "import dashboard_report as dr; print len(dr._label_to_field_map) - 1"`

    # We are manually going to create json to send to dashboard_report.
    # Since the url we fetch gives back json already, it's easy to turn
    # it into a bigger json struct.
    echo '['
    for chartnum in `seq 0 $num_charts`; do
        # window=0 gives us 30 minutes of data (cf. dashboard_report.py:_time_windows)
        window=0
        url="/dashboard/stats?app_id=${app_id}&version_id=&type=${chartnum}&window=${window}"
        echo '{"chart_num": '$chartnum', '
        echo ' "time_window": '$window', '
        echo ' "chart_url_data": '
        "${curl_app}" "${url}" "${username}" < "${private_pw}"
        echo "},"
    done
    # Darn json and its requirement the last list element doesn't have
    # a trailing comma.  Easiest way around this is to have a sentinel.
    echo 'null'
    echo ']'
} | "${srcdir}/dashboard_report.py" ${report_opts} ${start_time} ${end_time}
