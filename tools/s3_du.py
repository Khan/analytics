#!/usr/bin/env python
"""Lists disk usage of date partitionsn under an Amazon S3 path.

USAGE:
./s3_du.py base_path start_dt end_dt

E.g.,
./s3_du.py s3://ka-mapreduce/entity_store/UserDataP 2013-08-01 2013-08-05

"""
import datetime
import subprocess
import sys

DATE_FORMAT = "%Y-%m-%d"

base_path, start_dt, end_dt = sys.argv[1:4]

start_dt = datetime.datetime.strptime(start_dt, DATE_FORMAT)
end_dt = datetime.datetime.strptime(end_dt, DATE_FORMAT)

date = start_dt
while date < end_dt:
    cmd = 's3cmd du %s/dt=%s' % (base_path, date.strftime(DATE_FORMAT))
    cmd_output = subprocess.check_output(cmd, shell=True)
    print cmd_output,
    date += datetime.timedelta(days=1)
