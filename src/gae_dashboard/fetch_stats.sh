#!/bin/sh

timestamp=`date +%s`
srcdir="${HOME}/analytics/src/gae_dashboard"
private_pw="${HOME}/private_pw"
username="khanbackups@gmail.com"
url="https://appengine.google.com/instances?&app_id=s~khan-academy"

if [ ! -e "${private_pw}" ]; then
    echo "Need to put password for ${username} in ${private_pw}"
    exit 1
fi

"${srcdir}/gae_dashboard_curl.py" "${url}" "${username}" \
    < "${private_pw}" \
    | "${srcdir}/instance_report.py" ${timestamp}
