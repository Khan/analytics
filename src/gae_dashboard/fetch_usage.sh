#!/bin/sh

# This script is meant to be run as a cron job each morning (PST) to download
# daily usage reports from the GAE dashboard and load them into mongo.

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
    | "${srcdir}/load_usage_reports.py"
