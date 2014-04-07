#!/usr/bin/env python

"""Extract statistics from App Engine's /instance_summary dashboard
and store them in the analytics database.

The statistics are stored in the mongo collection
"gae_dashboard_instance_reports", and each record has the structure:

    { utc_datetime: DATETIME,
      num_instances: INTEGER,
      average_qps: FLOAT,
      average_latency_ms: FLOAT,
      average_memory_mb: FLOAT }
"""

import argparse
import datetime
import sys

import pymongo

import parsers


def _mongo_collection():
    """The pymongo.Collection where statistics are stored."""
    db = pymongo.Connection('107.21.23.204')
    collection = db['report']['gae_dashboard_instance_reports']
    return collection


def parse_and_commit_record(input_html, download_dt, verbose=False,
                            dry_run=False):
    """Parse and store instance summary data.

    Arguments:
      input_html: HTML contents of App Engine's /instance_summary
        dashboard.
      download_dt: Datetime when /instance_summary was downloaded.
      verbose: If True, print report to stdout.
      dry_run: If True, do not store report in the database.
    """
    summary = parsers.InstanceSummary(input_html).summary_dict()
    record = {'utc_datetime': download_dt,
              'num_instances': summary['total_instances'],
              'average_qps': summary['average_qps'],
              'average_latency_ms': summary['average_latency_ms'],
              'average_memory_mb': summary['average_memory_mb'],
             }
    if verbose:
        print record

    if not dry_run:
        _mongo_collection().insert(record)


def main():
    parser = argparse.ArgumentParser(description=__doc__.split('\n\n', 1)[0])
    parser.add_argument('unix_timestamp', type=int,
                        help='time_t the input data was downloaded')
    parser.add_argument('infile', nargs='?', type=argparse.FileType('r'),
                        default=sys.stdin,
                        help=("HTML contents of App Engine's /instances "
                              "[default: read from stdin]"))
    parser.add_argument('-v', '--verbose', action='store_true', default=False,
                        help='print report on stdout')
    parser.add_argument('-n', '--dry-run', action='store_true', default=False,
                        help='do not store report in the database')
    args = parser.parse_args()
    input_html = args.infile.read()
    download_dt = datetime.datetime.utcfromtimestamp(args.unix_timestamp)
    parse_and_commit_record(input_html, download_dt,
                            args.verbose, args.dry_run)


if __name__ == '__main__':
    main()
