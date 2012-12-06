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

USAGE = 'Usage: %prog [options] UNIX_TIMESTAMP <memcache.html'
DESCRIPTION = """UNIX_TIMESTAMP is the time_t at which memcache.html
was downloaded and is stored on the record in the analytics database."""

import datetime
import optparse
import sys

import pymongo

import parsers


def _mongo_collection():
    """The pymongo.Collection where statistics are stored."""
    db = pymongo.Connection('107.21.23.204')
    collection = db['report']['gae_dashboard_memcache_reports']
    return collection


def parse_and_commit_record(input_html, download_dt, debug=False):
    """Parse and store memcache summary data.

    Arguments:
      input_html: HTML contents of App Engine's /memcache dashboard.
      download_dt: Datetime when /memcache was downloaded.
      debug: If True, print but do not store the report.
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
    if debug:
        print record
    else:
        _mongo_collection().insert(record)


def main():
    parser = optparse.OptionParser(usage=USAGE, description=DESCRIPTION)
    parser.add_option('--debug', action='store_true',
                      dest='debug', default=False,
                      help='print report to stdout but do not write to '
                           'the database')
    (options, args) = parser.parse_args()
    if len(args) != 1:
        parser.error('UNIX_TIMESTAMP is required')
    else:
        download_dt = datetime.datetime.utcfromtimestamp(int(args[0]))
    input_html = sys.stdin.read()
    parse_and_commit_record(input_html, download_dt, options.debug)


if __name__ == '__main__':
    main()
