#!/usr/bin/env python

"""Extract statistics from the charts on App Engine's /dashboard page
and store them in the analytics database.

This script considers the most recently added records in the database
and only adds records that are more recent, up to 6 hours of data.

TODO(chris): this method used to collect the data across all graphs
for the "6 hrs" period, but currently only the "Summary" graph is
rendered into the page. In the future, we should instead hit the
"/stats" endpoint that client JavaScript loads when switching between
graphs.

CAVEAT: sometimes a subsequent run will log the same data points as a
new record.  The heart of the problem is that the x-axis of the
scraped 6-hour graphs covers 21,600 seconds, but the charting data may
only span 4095 values.  Thus each data point is roughly within a
bucket of 21,600 / 4095 ~= 5 seconds.  Furthermore, when unpacking
data from different graphs, the timestamps do not necessarily line up
with each other.  A single data point might appear in one record on
one run and in another record on a different run since we batch data
points into records by timestamp.

Records are stored in the collection "gae_dashboard_chart_reports",
and each has the structure:

    { utc_datetime: DATETIME,
      # From the chart "Summary"
      requests_per_second: FLOAT,
      errors_per_second: FLOAT,

      # From the chart "Requests by Type/Second"
      static_requests_per_second: FLOAT,
      dynamic_requests_per_second: FLOAT,
      cached_requests_per_second: FLOAT,
      pagespeed_requests_per_second: FLOAT,

      # From the chart "Latency"
      milliseconds_per_dynamic_request: FLOAT,

      # From the chart "Loading Latency"
      milliseconds_per_loading_request: FLOAT,

      # From the chart "Traffic (Bytes/Second)"
      bytes_received_per_second: FLOAT,
      bytes_sent_per_second: FLOAT,

      # From the chart "Utilization"
      total_cpu_seconds_used_per_second: FLOAT,
      api_cpu_seconds_used_per_second: FLOAT,

      # From the chart "Milliseconds Used/Second"
      milliseconds_used_per_second: FLOAT,

      # From the chart "Error Details"
      quota_denials_per_second: FLOAT,
      dos_api_denials_per_second: FLOAT,
      client_errors_per_second: FLOAT,
      server_errors_per_second: FLOAT,

      # From the chart "Instances".
      total_instance_count: FLOAT,
      active_instance_count: FLOAT,
      billed_instance_count: FLOAT,

      # From the chart "Memory Usage (MB)".
      memory_usage_mb: FLOAT }

"""

import argparse
import calendar
import datetime
import sys

import GChartWrapper
import pymongo

import graphite_util
import parsers


def _mongo_collection():
    """The pymongo.Collection where records are stored."""
    db = pymongo.Connection('107.21.23.204')
    collection = db['report']['gae_dashboard_chart_reports']
    return collection


def _time_t_of_latest_record(collection):
    """time_t of the most recently stored dashboard record.

    Arguments:
      collection: pymongo.Collection where records are stored.

    Returns:
      The time_t (# of seconds since the UNIX epoch in UTC) or None if
      there is no previous record.
    """
    partial_doc = collection.find_one(None,
                                      ['utc_datetime'],
                                      sort=[('utc_datetime', -1)])
    if not partial_doc:
        return None
    return calendar.timegm(partial_doc['utc_datetime'].utctimetuple())


def round_to_n_significant_digits(x, n):
    """Round a number to have no more than n significant digits.

    This is different than the built-in round() which rounds to n
    digits after the decimal place and is ignorant to significance.
    """
    if n < 1:
        raise ValueError('Number of significant digits must be >= 1')
    return float('%.*e' % (n - 1, x))


# This mapping is used to turn chart labels and possibly data labels
# into field names on the record to save. There are two rules:
#
#   1) If the chart label maps to a string then the string is the
#   field name for the lone series in the chart.
#
#   2) If the label maps to a dictionary then the dictionary's keys
#   map from the chart's data labels (the "chdl" query parameter) to
#   the field names for the named series data.
_label_to_field_map = {
    'Summary': {
        'Total Errors': 'errors_per_second',
        'Total Requests': 'requests_per_second',
        },
    'Requests by Type/Second': {
        'Static Requests': 'static_requests_per_second',
        'Dynamic Requests': 'dynamic_requests_per_second',
        'Cached Requests': 'cached_requests_per_second',
        'PageSpeed Requests': 'pagespeed_requests_per_second',
        },
    'Latency': 'milliseconds_per_dynamic_request',
    'Loading Latency': 'milliseconds_per_loading_request',
    'Error Details': {
        'Client (4xx)': 'client_errors_per_second',
        'Server (5xx)': 'server_errors_per_second',
        'Quota Denials': 'quota_denials_per_second',
        'DoS API Denials': 'dos_api_denials_per_second',
        },
    'Traffic (Bytes/Second)': {
        'Send': 'bytes_sent_per_second',
        'Received': 'bytes_received_per_second',
        },
    'Utilization': {
        'Total CPU': 'total_cpu_seconds_used_per_second',
        'API Calls CPU': 'api_cpu_seconds_used_per_second',
        },
    'Milliseconds Used/Second': 'milliseconds_used_per_second',
    'Instances': {
        'Total': 'total_instance_count',
        'Active': 'active_instance_count',
        'Billed': 'billed_instance_count',
        },
    'Memory Usage (MB)': 'memory_usage_mb',
    }


def lookup_field_name(chart_label, series_label=None):
    """Map a series in a well-known chart to the analytics database
    field name used to store its data points."""
    field_name = _label_to_field_map[chart_label]
    if isinstance(field_name, basestring):
        return field_name
    else:
        return field_name[series_label]


def get_axis_labels(chart):
    """Extract the axis labels from a Google Chart.

    In terms of the Google Chart API parameters, this uses "chxl" and
    "chxt" to determine the order of a graph's axes and their labels.

    Arguments:
      chart: a GChartWrapper.GChart instance.

    Returns:
      A dict mapping axis name to an ordered list of labels.

      Specifically, "chxl" indexes into "chxt".  Given Chart API
      parameters like these:

        chxt=x,y
        chxl=0:|now|-6hr|-12hr|-18hr|-1d|1:|400.00|800.00|1200|1600|2000

      this function would return the structure:

        {"x": ["now", "-6hr", "-12hr", "-18hr", "-1d"],
         "y": ["400.00", "800.00", "1200", "1600", "2000"]}
    """
    axes = chart['chxt'].split(',')
    axis_labels = [[] for _ in axes]
    axis_index = 0
    for label in chart['chxl'].split('|'):
        if label.endswith(':'):
            axis_index = int(label[:1])
            assert 0 <= axis_index < len(axes), axis_index
        else:
            axis_labels[axis_index].append(label)
    return dict((axis, axis_labels[i]) for i, axis in enumerate(axes))


def unpack_chart_data(url, time_delta_seconds):
    """Extract one or more data series from URL to the Google Chart API.

    Arguments:
      url: URL of a request to the Google Chart API.  Data is assumed
        to be encoded using extended format, x-axis is assumed to be
        time, and the y-axis is assumed to be the value.
      time_delta_seconds: the number of seconds the chart spans.  For
        example, this would be 21600 for one of the 6-hour charts.

    Returns:
      One or more pairs of (series_label, [(x1, y1), (x2, y2), ...])
      where series_label is either None when there is no label, or
      something like "Dynamic Requests" when there is.  In general,
      the charts on the App Engine dashboard have series labels when
      they contain multiple series.  The x, y pairs are the data
      points in the series.
    """
    chart = GChartWrapper.GChart.fromurl(url)
    # We expect extended data format (fromurl() doesn't detect data format).
    assert chart['chd'].startswith('e:'), url
    chart.encoding('extended')

    axis_labels = get_axis_labels(chart)
    value_max = float(axis_labels["y"][-1])

    # chart.getdata() returns a list of lists that alternate x-axis
    # (time), y-axis (value) per series.  For example:
    #
    #   [series1_x_values, series1_y_values, series2_x_values,
    #    series2_y_values, ...]
    #
    # Technically the series are ordered as described by Google Chart
    # API's "chxt" parameter. In practice App Engine charts have chxt=x,y
    series = chart.getdata()
    assert chart['chxt'] == 'x,y', chart['chxt']

    series_labels = chart['chdl'].split('|') if 'chdl' in chart else [None]
    assert 2 * len(series_labels) == len(series), (series_labels, series)
    for i in xrange(len(series_labels)):
        series_label = series_labels[i]
        time_series = series[i * 2]
        value_series = series[i * 2 + 1]
        # Extended encoding chart values go from 0 to 4095 either
        # left-to-right or bottom-to-top.  We assume data points are
        # approximate and choose to round time to the nearest second.
        # We round y values to 4 signficant digits, the best case
        # precision of the extended format.
        time_series = [int(float(x) / 4095 * time_delta_seconds)
                       for x in time_series]
        value_series = [
            round_to_n_significant_digits(float(y) / 4095 * value_max, 4)
            for y in value_series]
        xy_pairs = zip(time_series, value_series)
        yield series_label, xy_pairs


def aggregate_series_by_time(named_series):
    """Restructure multiple series of data by aggregating values at a
    given point in time into a record whose field names match the
    names of the series.

    Arguments:
      named_series: a dict mapping {"name": [(x1, y1), (x2, y2), ...]}
        The x values are assumed to represent time.

    Returns:
      A generator yielding (x, record) pairs where the record is a
      dict whose fields are the series values aggregated on unique x
      values.  Specifically, pairs of (x, {"series1": y1, "series2":
      y2, ...}).  A real example of a single pair for the App Engine
      Instances chart might look like this, after the series names are
      filtered through the lookup table in this module.

        (123, {"total_instance_count": 100,
               "active_instance_count": 45,
               "billed_instance_count": 100})
    """
    x_values = set()
    for name, xy_pairs in named_series.iteritems():
        x_values.update(x for x, y in xy_pairs)

    for x_value in sorted(x_values):
        record = {}
        for name, xy_pairs in named_series.iteritems():
            matched_pairs = filter(lambda (x, y): x == x_value, xy_pairs)
            assert len(matched_pairs) in (0, 1), matched_pairs
            if matched_pairs:
                record[name] = matched_pairs[0][1]  # y of [(x, y)]
        yield x_value, record


def parse_and_commit_record(input_html, download_time_t, graphite_host='',
                            verbose=False, dry_run=False):
    """Parse and store dashboard chart data.

    Arguments:
      input_html: HTML contents of App Engine's /dashboard dashboard.
      download_time_t: When /dashboard was downloaded in seconds (UTC).
      graphite_host: host:port of graphite server to send data to, or ''/None
      verbose: If True, print report to stdout.
      dry_run: If True, do not store report in the database.
    """
    time_label, time_delta = '24 hrs', datetime.timedelta(hours=24)
    chart_start_time_t = download_time_t - time_delta.total_seconds()

    # Extract named time series data from the raw HTML.
    named_series = {}
    charts = parsers.Dashboard(input_html).charts(time_label)
    for chart_label, url in charts:
        chart_data = unpack_chart_data(url, time_delta.total_seconds())
        for series_label, xy_pairs in chart_data:
            field_name = lookup_field_name(chart_label, series_label)
            named_series[field_name] = xy_pairs

    # Determine the starting point for records we want to add.  This
    # script may be run by cron and fetches a minimum of 6 hours of
    # data, but chances are good that it runs more frequently.
    mongo_collection = _mongo_collection()
    time_t_of_latest_record = _time_t_of_latest_record(mongo_collection)
    if time_t_of_latest_record is None:
        print >>sys.stderr, ('No dashboard records found in mongo. '
                             'Importing all records as new.')
        time_t_of_latest_record = 0

    # Build time-keyed records from the named time series data and
    # decide which records will be stored.
    records = []
    for time_value, record in aggregate_series_by_time(named_series):
        record_time_t = chart_start_time_t + time_value
        if record_time_t > time_t_of_latest_record:
            record['utc_datetime'] = datetime.datetime.utcfromtimestamp(
                record_time_t)
            records.append(record)

    if verbose:
        print records

    print >>sys.stderr, 'Importing %d record%s' % (len(records),
                                                   's'[len(records) == 1:])
    if dry_run:
        print >>sys.stderr, 'Skipping import during dry-run.'
    elif records:
        # Do the graphite send first, since mongo modifies 'records' in place.
        graphite_util.maybe_send_to_graphite(graphite_host, 'summary', records)
        mongo_collection.insert(records)


def main():
    parser = argparse.ArgumentParser(description=__doc__.split('\n\n', 1)[0])
    parser.add_argument('unix_timestamp', type=int,
                        help='time_t the input data was downloaded')
    parser.add_argument('infile', nargs='?', type=argparse.FileType('r'),
                        default=sys.stdin,
                        help=("HTML contents of App Engine's /dashboard "
                              "[default: read from stdin]"))
    parser.add_argument('--graphite_host',
                        default='carbon.hostedgraphite.com:2004',
                        help=('host:port to send stats to graphite '
                              '(using the pickle protocol). '
                              'Use the empty string to disable graphite. '
                              '(Default: %(default)s)'))
    parser.add_argument('-v', '--verbose', action='store_true', default=False,
                        help='print report on stdout')
    parser.add_argument('-n', '--dry-run', action='store_true', default=False,
                        help='do not store report in the database')
    args = parser.parse_args()
    input_html = args.infile.read()
    parse_and_commit_record(input_html, args.unix_timestamp,
                            args.graphite_host, args.verbose, args.dry_run)


if __name__ == '__main__':
    main()
