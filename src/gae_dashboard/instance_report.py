#!/usr/bin/env python

"""Extract statistics from App Engine's /instances dashboard and store
them in the analytics database.

The statistics are stored in the mongo collection
"gae_dashboard_instance_reports", and each record has the structure:

    { utc_datetime: DATETIME,
      num_instances: INTEGER,
      average_qps: FLOAT,
      average_latency_ms: FLOAT,
      average_memory_mb: FLOAT }

Usage: instance_report.py UNIX_TIMESTAMP <instances.html

UNIX_TIMESTAMP is the timestamp at which instance.html was downloaded
and is stored on the record in the analytics database.
"""

import datetime
import sys

import pymongo

import parsers


def _mongo_collection():
    """The pymongo.Collection where statistics are stored."""
    db = pymongo.Connection('107.21.23.204')
    collection = db['report']['gae_dashboard_instance_reports']
    return collection


def main(input_html, download_dt):
    """Parse and store instance summary data.

    Arguments:
      input_html: HTML contents of App Engine's /instances dashboard.
      download_dt: Datetime when /instances was downloaded.
    """
    summary = parsers.Instances(input_html).summary_dict()
    record = {'utc_datetime': download_dt,
              'num_instances': summary['total_instances'],
              'average_qps': summary['average_qps'],
              'average_latency_ms': summary['average_latency_ms'],
              'average_memory_mb': summary['average_memory_mb'],
             }
    _mongo_collection().insert(record)


if __name__ == '__main__':
    input_html = sys.stdin.read()
    download_dt = datetime.datetime.utcfromtimestamp(int(sys.argv[1]))
    main(input_html, download_dt)
