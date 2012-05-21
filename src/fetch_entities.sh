#!/bin/bash

# This script is intended to be run daily by a cron job. It just fetches
# yesterday's problem logs and puts them in ~/kadata/daily.

: ${PYTHON:=python}

if [ -n "$1" ]; then
    day="$1"
else
    day=$(date --date='yesterday' '+%Y-%m-%d')
fi 
day_next=$(date --date="$day + 1 day" +'%Y-%m-%d')

if [ -n "$2" ]; then
    log_dir="$2"
else
    log_dir="$HOME/kadata/daily"
fi
mkdir -p "$log_dir"

ROOT="$(dirname $0)"

export PYTHONPATH="${ROOT}:${PYTHONPATH}"   # for oauth_util directory

"$PYTHON" "$ROOT/fetch_entities.py" \
    -s "${day}T00:00:00Z" -e "${day_next}T00:00:00Z" \
    -t ProblemLog -o "$log_dir/$day-ProblemLog.pickle" \
    >"$log_dir/$day-ProblemLog.log" 2>&1

gzip "$log_dir/$day-ProblemLog.pickle"

"$PYTHON" "$ROOT/fetch_entities.py" \
    -s "${day}T00:00:00Z" -e "${day_next}T00:00:00Z" \
    -t VideoLog -o "$log_dir/$day-VideoLog.pickle" \
    >"$log_dir/$day-VideoLog.log" 2>&1

gzip "$log_dir/$day-VideoLog.pickle"
