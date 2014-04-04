#!/usr/bin/env python

"""Extract statistics from App Engine's /memcache dashboard and store
them in the analytics database.

The statistics are stored in the mongo collection
"gae_dashboard_memcache_reports", and each record has the structure:

    { utc_datetime: DATETIME,
      hit_count: INTEGER,
      miss_count: INTEGER,
      hit_ratio: FLOAT,
      item_count: INTEGER,
      total_cache_size_bytes: INTEGER,
      oldest_item_age_seconds: INTEGER }
"""

import argparse
import datetime
import sys

import pymongo

import parsers


def _mongo_collection():
    """The pymongo.Collection where statistics are stored."""
    db = pymongo.Connection('107.21.23.204')
    collection = db['report']['gae_dashboard_memcache_reports']
    return collection


def parse_and_commit_record(input_html, download_dt, verbose=False,
                            dry_run=False):
    """Parse and store memcache summary data.

    Arguments:
      input_html: HTML contents of App Engine's /memcache dashboard.
      download_dt: Datetime when /memcache was downloaded.
      verbose: If True, print report to stdout.
      dry_run: If True, do not store report in the database.
    """
    summary = parsers.Memcache(input_html).statistics_dict()
    record = {'utc_datetime': download_dt,
              'hit_count': summary['hit_count'],
              'miss_count': summary['miss_count'],
              'hit_ratio': summary['hit_ratio'],
              'item_count': summary['item_count'],
              'total_cache_size_bytes': summary['total_cache_size_bytes'],
              'oldest_item_age_seconds': summary['oldest_item_age_seconds'],
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
                        help=("HTML contents of App Engine's /memcache "
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
