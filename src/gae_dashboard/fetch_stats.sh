#!/bin/sh

timestamp=`date +%s`
srcdir="${HOME}/analytics/src/gae_dashboard"
private_pw="${HOME}/private_pw"
username="khanbackups@gmail.com"
curl_app="${srcdir}/gae_dashboard_curl.py"
base_url="https://appengine.google.com"
app_id="s~khan-academy"

if [ ! -e "${private_pw}" ]; then
    echo "Need to put password for ${username} in ${private_pw}"
    exit 1
fi

"${curl_app}" "${base_url}/instances?app_id=${app_id}" "${username}" \
    < "${private_pw}" \
    | "${srcdir}/instance_report.py" ${timestamp}

"${curl_app}" "${base_url}/memcache?app_id=${app_id}" "${username}" \
    < "${private_pw}" \
    | "${srcdir}/memcache_report.py" ${timestamp}
