#!/usr/bin/env python
"""Utility script to archive local data to S3.

This is quick-n-dirty.  You can add configuration if you want, but
for right now it scans relevant directories for data that is older than
15 weeks.
"""

import datetime
import os
import subprocess

# How old the data needs to be before we archive it
STALE_THRESHOLD = datetime.timedelta(weeks=15)


def run_shell_command(command_string, dry_run=False):
    print command_string
    if not dry_run:
        command_output = subprocess.check_output(command_string, shell=True)
        print command_output


def archive_daily_new(date_string):
    local_path = '/home/analytics/kabackup/daily_new/%s/' % date_string
    s3_path = 's3://ka-mapreduce/rawdata/%s/' % date_string

    if not os.path.exists(local_path):
        return

    # Note that if this sync command returns a non-zero exit code,
    # subprocess.check_ouput will throw an exception, and this script
    # will abort (and send email of run by cron), without deleteing any
    # further data.
    cmd = 's3cmd sync %s %s' % (local_path, s3_path)
    run_shell_command(cmd)

    cmd = 'rm -rf %s' % (local_path,)
    run_shell_command(cmd)


def main():
    today = datetime.datetime.now()
    archive_end = today - STALE_THRESHOLD
    archive_start = archive_end - datetime.timedelta(weeks=52)

    date = archive_start
    while date < archive_end:
        date_string = date.strftime('%Y-%m-%d')
        archive_daily_new(date_string)
        date += datetime.timedelta(days=1)


if __name__ == '__main__':
    main()