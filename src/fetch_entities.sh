#!/bin/bash

# This script is intended to be run daily by a cron job. It just fetches
# yesterday's problem logs and puts them in ~/data/daily.

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

export PYTHONPATH="$ROOT/..:$PYTHONPATH"

"$PYTHON" "$ROOT/fetch_entities.py" \
    -s "${day}T00:00:00Z" -e "${day_next}T00:00:00Z" \
    -t ProblemLog -o "$log_dir/$day-ProblemLog.pickle" \
    >"$log_dir/$day-ProblemLog.log" 2>&1

"$PYTHON" "$ROOT/../src/py/ka_mongo_loader.py" -d "$log_dir" \
    -f "$log_dir/$day-ProblemLog.pickle" -t PBUF \
    > "$log_dir/$day-ProblemLog.loader.log" 2>&1 \
    && gzip "$log_dir/$day-ProblemLog.pickle"

#TODO(jace): analysis should not be embedded here, but its a hacky way
#to ensure is runs as soon as the data is possible.
python "$ROOT/../src/py/ka_daily_exercise_stats.py"

"$PYTHON" "$ROOT/fetch_entities.py" \
    -s "${day}T00:00:00Z" -e "${day_next}T00:00:00Z" \
    -t VideoLog -o "$log_dir/$day-VideoLog.pickle" \
    >"$log_dir/$day-VideoLog.log" 2>&1

"$PYTHON" "$ROOT/../src/py/ka_mongo_loader.py" -d "$log_dir" \
    -f "$log_dir/$day-VideoLog.pickle" -t PBUF \
    > "$log_dir/$day-VideoLog.loader.log" 2>&1 \
    && gzip "$log_dir/$day-VideoLog.pickle"
