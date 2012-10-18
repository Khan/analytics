#!/usr/bin/env python

"""
This script reads an App Engine usage report in CSV from input then detects
which data isn't already in mongo db and if any is found, loads it into mongo.

Usage:

  ./load_usage_reports.py < billing_history.csv


The expected CSV input looks like what /billing/history.csv?app_id= returned as
of October 2012:

  Date,Name,Unit,Used
  2012-10-11,Frontend Instance Hours,Hour,"X,XXX.XX"
  2012-10-11,Backend Instance Hours,Hour,XXX.XX
  ...

The output describes which data was loaded into mongo db.

Data is loaded into the mongo collection gae_dashboard_usage_reports as
documents with the following structure:

  { dt: '2012-10-11',
    usage: [{'name': <STRING>, 'unit': <STRING>, 'used': <NUMBER>}, ... ] }
"""
import csv
import pymongo
import sys


def _mongo_collection():
    """The pymongo.Collection to store reports in."""
    db = pymongo.Connection('107.21.23.204')
    collection = db['reporting']['gae_dashboard_usage_reports']
    return collection


def _dt_of_latest_report(collection):
    """Return date string of the most recent usage report in mongo.

    The date string is in the 'dt' format, e.g., 2012-10-11.
    """
    partial_doc = collection.find_one(None, ['dt'], sort=[('dt', -1)])
    if not partial_doc:
        return None
    return partial_doc['dt']


def _reports_since_dt(csvreader, cutoff_dt=None):
    """Return usage reports that are newer than the given cutoff point.

    Arguments:
        csvreader: an iterable where each iteration returns the usage report
            fields (Date, Name, Unit, Used).
        cutoff_dt (optional): a date string in the format 'YYYY-MM-DD'.

    Returns:
      The returned object is an iterator. Each iteration returns the tuple
      (dt, usage_list) where each element of "usage_list" is a dictionary
      whose fields match the CSV usage data: 'name', 'unit', and 'used'.

      For example:

      ('2012-10-15', [{'name': 'Frontend', 'unit': 'Hour', 'used': XX.XX},
                      {'name', 'Backend', 'unit': 'Hour', 'used': XX.XX},
                      ...])
    """
    last_dt = None
    usage_list = []
    for row in csvreader:
        if not row:
            # Skip blank lines.
            continue
        dt, name, unit, used = row
        # Assume that all entries for a given date are contiguous. Then a
        # report is complete when the date changes.
        #
        # NOTE: reports might not be in order. For example, the lines for
        # 2012-10-09 might be before those for 2012-10-13. Because of this, we
        # iterate over all of the input rather than stopping at a certain
        # point. This is fine since the input is at most a few months of data,
        # as of October 2012.
        if dt != last_dt and last_dt > cutoff_dt and usage_list:
            yield (last_dt, usage_list)
            usage_list = []
        str_used = used.replace(',', '')  # float/int can't parse a comma
        num_used = float(str_used) if '.' in str_used else int(str_used)
        usage_list.append({'name': name, 'unit': unit, 'used': num_used})
        last_dt = dt


def main(csv_iter):
    """Parse App Engine usage report CSV and bring a mongo db collection
    up-to-date with it.

    csv_input is any object that returns a line of the usage report CSV for
    each iteration. This includes the header line containing field names.
    """
    csvreader = csv.reader(csv_iter)

    # This is a sanity check of the usage report format. If the fields have
    # changed then this script probably needs to be updated.
    first_line = csvreader.next()
    assert first_line == ['Date', 'Name', 'Unit', 'Used'], \
           'unexpected fields: %r' % first_line

    mongo_collection = _mongo_collection()

    latest_dt = _dt_of_latest_report(mongo_collection)
    if latest_dt is None:
        print 'No usage reports found in mongo. Importing all usage data.'
    else:
        print 'Importing usage reports newer than %s' % latest_dt

    docs_to_add = []
    for dt, usage_list in _reports_since_dt(csvreader, latest_dt):
        print 'Found usage report for %s' % dt
        docs_to_add.append({'dt': dt, 'usage': usage_list})

    print 'Importing %s documents' % len(docs_to_add)
    if docs_to_add:
        mongo_collection.insert(docs_to_add, safe=True)


if __name__ == "__main__":
    main(sys.stdin)
