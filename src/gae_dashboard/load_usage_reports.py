#!/usr/bin/env python

"""Send statistics from App Engine's usage reports to graphite.

This script reads an App Engine usage report in CSV format from stdin,
then takes all records since the input start_date, parses them, and
emits the data to graphite.

Usage:
  ./load_usage_reports.py < billing_history.csv

The expected CSV input looks like what /billing/history.csv?app_id=
returned as of October 2012:

  Date,Name,Unit,Used
  2012-10-11,Frontend Instance Hours,Hour,"X,XXX.XX"
  2012-10-11,Backend Instance Hours,Hour,XXX.XX
  ...

Records are sent to graphite under the keys webapp.gae.dashboard.usage.*
"""

import argparse
import csv
import datetime
import re
import sys

import graphite_util


def _munge_key(key):
    """Turns the key into a graphite-friendly key name.

    The GAE report has names like "Search Document Storage" and
    "Dedicated Memcache (10k ops/s/GB, 1GB unit)".  Thse are not
    friendly keys for graphite!  We convert them into names like
    search_document_storage and dedicated_memcache, which is not
    great but much better.
    """
    # Ignore any parentheticals.
    key = re.sub('[^\w\s].*', '', key)
    key = re.sub('\s+', '_', key.strip())
    return key.lower()


def _reports_since_dt(csvreader, cutoff_dt):
    """Return usage reports that are from the given cutoff date or later.

    Arguments:
        csvreader: an iterable where each iteration returns the usage report
            fields as a dict, with keys: Date, Name, Unit, Used.
        cutoff_dt: a string of the form 'YYYY-MM-DD'.  All records from
            cvsreader that have a date before cutoff_dt are ignored.

    Returns:
      An iterator where each element is a triple (<date>, <key>, <value>).
      Value is a float or an int.

      For example:
         (datetime.datetime(2012, 10, 15, 0, 0, 0), 'Frontend', 31000.23)
    """
    seen_dts = set()
    for row in csvreader:
        if not row:
            # Skip blank lines.
            continue

        # NOTE: reports might not be in order. For example, the lines for
        # 2012-10-09 might be before those for 2012-10-13. Because of this, we
        # iterate over all of the input rather than stopping at a certain
        # point. This is fine since the input is at most a few months of data,
        # as of October 2012.
        if row['Date'] < cutoff_dt:
            continue

        if row['Date'] not in seen_dts:
            print 'Found usage report for: %s' % row['Date']
            seen_dts.add(row['Date'])

        used_str = row['Used'].replace(',', '')  # float() can't parse a comma
        used_num = float(used_str) if '.' in used_str else int(used_str)
        yield (datetime.datetime.strptime(row['Date'], '%Y-%m-%d'),
               row['Name'],
               used_num)


def main(csv_iter):
    """Parse App Engine usage report CSV and bring a mongo db collection
    up-to-date with it.

    csv_input is any object that returns a line of the usage report CSV for
    each iteration. This includes the header line containing field names.
    """
    yesterday = datetime.datetime.utcnow() - datetime.timedelta(days=1)

    parser = argparse.ArgumentParser(description=__doc__.split('\n\n', 1)[0])
    parser.add_argument('start_date', nargs='?',
                        default=yesterday.strftime('%Y-%m-%d'),
                        help=('Ignore data before this date (YYYY-MM-DD) '
                              '[default: %(default)s]'))
    parser.add_argument('--graphite_host',
                        default='carbon.hostedgraphite.com:2004',
                        help=('host:port to send stats to graphite '
                              '(using the pickle protocol). '
                              '[default: %(default)s]'))
    parser.add_argument('-v', '--verbose', action='store_true', default=False,
                        help='print report on stdout')
    parser.add_argument('-n', '--dry-run', action='store_true', default=False,
                        help='do not store report in the database')
    args = parser.parse_args()

    csvreader = csv.DictReader(csv_iter)

    print 'Importing usage reports starting from %s' % args.start_date

    records_to_add = []
    for (dt, key, value) in _reports_since_dt(csvreader, args.start_date):
        records_to_add.append({'utc_datetime': dt, _munge_key(key): value})

    if args.verbose:
        print records_to_add

    print 'Importing %s documents' % len(records_to_add)

    if args.dry_run:
        print >>sys.stderr, 'Skipping import during dry-run.'
    elif records_to_add:
        graphite_util.maybe_send_to_graphite(args.graphite_host, 'usage',
                                             records_to_add)


if __name__ == "__main__":
    main(sys.stdin)
