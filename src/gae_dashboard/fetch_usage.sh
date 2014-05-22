#!/bin/sh

# This script is meant to be run as a cron job each morning (PST) to download
# daily usage reports from the GAE dashboard and send them to graphite.

set -e

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
: ${url:="https://appengine.google.com/billing/history.csv?app_id=s~khan-academy"}

if [ ! -e "${private_pw}" ]; then
    echo "Need to put password for ${username} in ${private_pw}"
    exit 1
fi

"${srcdir}/gae_dashboard_curl.py" "${url}" "${username}" \
    < "${private_pw}" \
    | "${srcdir}/load_usage_reports.py" ${report_opts}
