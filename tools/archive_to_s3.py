#!/usr/bin/env python
"""Utility script to archive local data to S3.

NOTE(jace): This is quick-n-dirty, and not fully baked.  You can add
configuration to this script if you want, but for right now it scans relevant
directories for data that are older than
3 months.  I recommend running this in dry run mode, and just producing
command output.  Then you can verify those commands are working properly
before running over a large group of dates.
"""

import datetime
import os
import subprocess

# How old the data needs to be before we archive it
STALE_THRESHOLD = datetime.timedelta(weeks=30)


def run_shell_command(command_string, dry_run=True):
    print command_string
    if not dry_run:
        command_output = subprocess.check_output(command_string, shell=True)
        print command_output


def date_as_path(date_string):
    # convert "YYYY-MM-DD" to "YYYY/MM/DD"
    return date_string.replace('-', '/')


def archive_kalogs(date_string):
    local_path = '/home/analytics/kalogs/%s/' % date_as_path(date_string)
    s3_path = 's3://ka-mapreduce/rawdata/server_logs/website/%s/' % date_string

    if not os.path.exists(local_path):
        return

    cmd = 's3cmd sync %s %s' % (local_path, s3_path)
    run_shell_command(cmd, True)

    cmd = 'rm -rf %s' % (local_path, True)
    run_shell_command(cmd)


def archive_daily_new(date_string):
    local_path = '/home/analytics/kabackup/daily_new/%s/' % date_string
    s3_path = 's3://ka-mapreduce/rawdata/%s/' % date_string

    if not os.path.exists(local_path):
        return

    cmd = 's3cmd sync %s %s' % (local_path, s3_path)
    run_shell_command(cmd, True)

    cmd = 'rm -rf %s' % (local_path, True)
    run_shell_command(cmd)


def main():
    today = datetime.datetime.now()
    archive_end = today - STALE_THRESHOLD
    archive_start = archive_end - datetime.timedelta(weeks=52)

    date = archive_start
    while date < archive_end:
        date_string = date.strftime('%Y-%m-%d')
        archive_kalogs(date_string)
        date += datetime.timedelta(days=1)


if __name__ == '__main__':
    main()
