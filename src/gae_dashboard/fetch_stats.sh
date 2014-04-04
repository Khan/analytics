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
: ${base_url:="https://appengine.google.com"}
: ${app_id:="s~khan-academy"}

if [ ! -e "${private_pw}" ]; then
    echo "Need to put password for ${username} in ${private_pw}"
    exit 1
fi

timestamp=`date +%s`
"${curl_app}" "${base_url}/instances?app_id=${app_id}" "${username}" \
    < "${private_pw}" \
    | "${srcdir}/instance_report.py" ${report_opts} ${timestamp}

timestamp=`date +%s`
"${curl_app}" "${base_url}/memcache?app_id=${app_id}" "${username}" \
    < "${private_pw}" \
    | "${srcdir}/memcache_report.py" ${report_opts} ${timestamp}

timestamp=`date +%s`
"${curl_app}" "${base_url}/dashboard?app_id=${app_id}" "${username}" \
    < "${private_pw}" \
    | "${srcdir}/dashboard_report.py" ${report_opts} ${timestamp}
