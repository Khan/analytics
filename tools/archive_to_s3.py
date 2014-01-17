#!/usr/bin/env python
"""Utility script to archive local data to S3.

NOTE(jace): This is pretty quick-n-dirty, to end the increasing amount
of 'low disk space' warnings we get on the analytics machine.  You can add
configuration to this script if you want, but for right now it scans relevant
directories for data that are older than constant numbers of weeks.
By default, this script runs in "dry_run" mode, which means is will
print some command to stdout, but will not actually execute them. If you
pass 'I_AM_SERIOUS' as the first command line argument to the script, though,
the commands will execute, including removal of data!
"""

import datetime
import os
import re
import subprocess
import sys

dry_run = True

# How old the datastore entity data needs to be before we archive it
STALE_THRESHOLD_ENTITIES = datetime.timedelta(weeks=10)
# How old the RequestLog/AppLog data needs to be before we archive it
STALE_THRESHOLD_LOGS = datetime.timedelta(weeks=15)


def run_shell_command(command_string):
    print command_string
    if not dry_run:
        subprocess.check_output(command_string, shell=True)


def date_as_path(date_string):
    # convert "YYYY-MM-DD" to "YYYY/MM/DD"
    return date_string.replace('-', '/')


def archive_logs_day(date_string):
    local_path = '/home/analytics/kalogs/%s/' % date_as_path(date_string)
    s3_path = 's3://ka-mapreduce/rawdata/server_logs/website/%s/' % date_string

    if not os.path.exists(local_path):
        return

    cmd = '/usr/local/bin/s3cmd sync %s %s' % (local_path, s3_path)
    run_shell_command(cmd)

    # assertion for safety, before doing `rm -rf`
    assert re.match('/home/analytics/kalogs/.+/', local_path)
    cmd = 'rm -rf %s' % (local_path)
    run_shell_command(cmd)


def archive_entities_day(date_string):
    local_path = '/home/analytics/kabackup/daily_new/%s/' % date_string
    s3_path = 's3://ka-mapreduce/rawdata/%s/' % date_string

    if not os.path.exists(local_path):
        return

    cmd = '/usr/local/bin/s3cmd sync %s %s' % (local_path, s3_path)
    run_shell_command(cmd)

    # assertion for safety, before doing `rm -rf`
    assert re.match('/home/analytics/kabackup/daily_new/.+/', local_path)
    cmd = 'rm -rf %s' % (local_path)
    run_shell_command(cmd)


def archive_data(archive_day_function, timedelta_to_keep):
    today = datetime.datetime.now()
    archive_end = today - timedelta_to_keep
    archive_start = archive_end - datetime.timedelta(weeks=52)

    date = archive_start
    while date < archive_end:
        date_string = date.strftime('%Y-%m-%d')
        archive_day_function(date_string)
        date += datetime.timedelta(days=1)


def main():
    archive_data(archive_entities_day, STALE_THRESHOLD_ENTITIES)
    archive_data(archive_logs_day, STALE_THRESHOLD_LOGS)


if __name__ == '__main__':
    dry_run = len(sys.argv) <= 1 or sys.argv[1] != 'I_AM_SERIOUS'
    main()
