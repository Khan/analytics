#!/usr/bin/env python

"""A dashboard webapp.

This hosts the following dashboards:
- daily exercise statistics over time
- a daily report of videos watched
- learning efficiency and retention from exercises

It will house more dashboards for fundamental metrics we want to track.
"""

import datetime
import gzip
import json
import logging
import optparse
import os
import re

import flask
import pymongo

import auth
import data

app = flask.Flask(__name__)

# TODO(david): Allow specifying db params via cmd line args, and/or use
#     cfg/analytics.json for defaults.
db = pymongo.Connection('107.21.23.204')


@app.route('/')
@auth.login_required
def list_dashboards():
    return flask.render_template('index.html')


@app.route('/video-summary')
@auth.login_required
def videos_summary_dashboard():
    return flask.render_template('video-summary.html')


@app.route('/top-videos')
@auth.login_required
def videos_dashboard():
    return flask.render_template('top-videos.html')


@app.route('/video-topic-summary')
@auth.login_required
def video_topic_dashboard():
    return flask.render_template('video-topic-summary.html')


@app.route('/exercises')
@auth.login_required
def exercises_dashboard():
    return flask.render_template('daily-ex-stats.html')


@app.route('/growth')
@auth.login_required
def growth_dashboard():
    return flask.render_template('user-growth.html')


@app.route('/goals')
@auth.login_required
def goals_dashboard():
    return flask.render_template('company-goals.html')


@app.route('/learning')
@auth.login_required
def learning_dashboard():
    return flask.render_template('learning-stats.html')


@app.route('/data/topic_summary')
@auth.login_required
def topic_summary_data():
    dt = flask.request.args.get('start_date', '')
    duration = flask.request.args.get('time_scale', '')
    (top_results, second_results) = data.topic_summary(db, dt, duration)
    return flask.jsonify({'top_results': top_results,
                          'second_results': second_results})


@app.route('/data/top_videos')
@auth.login_required
def top_video_data():
    dt = flask.request.args.get('start_date', '')
    duration = flask.request.args.get('time_scale', '')
    results = data.top_videos(db, dt, duration)
    return flask.jsonify({'results': results})


@app.route('/data/video_title_summary')
@auth.login_required
def video_title_summary_data():
    start_dt = flask.request.args.get('start_date', '')
    end_dt = flask.request.args.get('end_date', '')
    title = flask.request.args.get('title', 'Total')
    duration = flask.request.args.get('time_scale', 'month')
    results = data.video_title_summary(db, title, duration, start_dt, end_dt)
    return flask.jsonify({'results': results})


# TODO(david): Add to analytics homepage after I get a nice screenshot
@app.route('/real-time')
def real_time_stats():
    return flask.render_template('real-time-stats.html')


@app.route('/db/distinct_video_titles')
@auth.login_required
def video_titles():
    video_titles = db.report.video_stats.distinct('title')
    return flask.jsonify({
        'video_titles': video_titles
    })


@app.route('/db/learning_stats_topics')
@auth.login_required
def learning_stats_topics():
    # TODO(david): Find a way of using the Khan API to find topics that have
    #     exercises in them. I tried to get this to work thru Sleepy Mongoose:
    #     https://github.com/kchodorow/sleepy.mongoose/wiki/database-commands
    topics = db.report.weekly_learning_stats.distinct('topic')
    return flask.jsonify({
        'topics': topics,
    })


@app.route('/db/<collection_name>/start_dates')
@auth.login_required
def collection_start_dates(collection_name):
    # TODO(david): Should actually return distinct date ranges, but now it's
    #     5 am and I have to sleep for 9:30 am scavenger hunt!
    collection = getattr(db.report, collection_name)
    return flask.jsonify({
        'start_dates': collection.distinct('start_dt')
    })


_billing_resources = (
    'Frontend Instance Hours', 'Discounted Instance Hour',
    'Backend Instance Hours', 'Datastore Storage', 'Logs Storage',
    'Taskqueue Storage', 'Blobstore Storage', 'Code and Static File Storage',
    'Datastore Writes', 'Datastore Reads', 'Small Datastore Operations',
    'Bandwidth Out', 'Emails', 'XMPP Stanzas', 'Opened Channels',
    'Logs Read Bandwidth', 'PageSpeed Out Bandwidth', 'SSL VIPs',
    'SSL SNI Certificates'
)


@app.route('/gae_stats/billing_history')
@auth.login_required
def gae_stats_billing_history():
    """Display usage over time for billable App Engine resources."""
    resource_name = flask.request.args.get('res', None)
    if resource_name not in _billing_resources:
        resource_name = _billing_resources[0]

    results = list(data.gae_usage_reports_for_resource(db, resource_name))

    if results:
        _, _, resource_unit = results[0]
    else:
        resource_unit = ''

    def result_iter():
        for dt, used, _ in results:
            # Convert 2012-10-11 to (2012, 9, 11) for use in the JavaScript
            # Date constructor.
            dt_parts = map(int, dt.split('-'))
            dt_parts[1] = dt_parts[1] - 1
            yield tuple(dt_parts), used

    return flask.render_template('gae-stats/billing-history.html',
                                 resource_name=resource_name,
                                 resource_unit=resource_unit,
                                 resources=_billing_resources,
                                 data=result_iter())


@app.route('/gae_stats/instances')
@auth.login_required
def gae_stats_instances():
    # tuple (('YYYY', 'MM', 'DD', 'HH', 'mm', 'SS'), num_instances)
    instance_counts = []
    for (root, _, files) in os.walk('/home/analytics/kadata/gae_dashboard'):
        for basename in files:
            filepath = os.path.join(root, basename)
            timestamp_tuple = re.findall(
                r'/(\d\d\d\d)/(\d\d)/(\d\d)/instances-(\d\d):(\d\d):(\d\d)',
                filepath)
            if timestamp_tuple:
                with gzip.open(filepath) as f:
                    try:
                        report = json.load(f)
                    except json.JSONError, e:
                        logging.warn('skipping file %s: %s' % (filepath, e))
                        continue
                # Convert to 0-indexed months for JavaScript Date constructor.
                date_parts = map(int, timestamp_tuple[0])
                date_parts[1] = date_parts[1] - 1
                instance_counts.append((tuple(date_parts), len(report)))
    return flask.render_template('gae-stats/instances.html',
                                 instance_counts=instance_counts)


@app.route('/gae_stats/daily_request_log_url_stats')
@auth.login_required
def gae_stats_daily_request_log_url_stats():
    """This dashboard shows stats for the most accessed URLs."""
    num_urls = int(flask.request.args.get('count', '100'))

    # Some days the data isn't generated properly, and some days
    # it takes a while for yesterday's report to be generated.  So
    # we try going back a few days.  When we go back far enough, we
    # say so in the date.
    for days_ago in xrange(1, 8):
        dt_string = utc_as_dt(days_ago)
        results = data.daily_request_log_url_stats(db, dt=dt_string,
                                                   limit=num_urls)
        if results.count():
            return flask.render_template(
                'gae-stats/daily-request-log-url-stats.html',
                collection_rows=results,
                count=num_urls, date=dt_string, days_ago=days_ago)
    return 'No data in the db for the last %s days' % days_ago


@app.route('/gae_stats/daily_request_log_urlroute_stats')
@auth.login_required
def gae_stats_daily_request_log_urlroute_stats():
    """This dashboard shows stats for the most accessed URLs, grouped by the
    route patterns that they match for handlers on the website.
    """
    def result_iter():
        # Set 'url' so that we can reuse the same template as
        # daily_request_log_url_stats.  This is done one-by-one in a
        # generator and not by iterating over the results here in
        # order to avoid exhausting the "results" cursor.
        for row in results:
            row['url'] = row['url_route']
            yield row

    num_urls = int(flask.request.args.get('count', '100'))

    # Some days the data isn't generated properly, and some days
    # it takes a while for yesterday's report to be generated.  So
    # we try going back a few days.  When we go back far enough, we
    # say so in the date.
    for days_ago in xrange(1, 8):
        dt_string = utc_as_dt(days_ago)
        results = data.daily_request_log_urlroute_stats(db, dt=dt_string,
                                                        limit=num_urls)
        if results.count():
            return flask.render_template(
                'gae-stats/daily-request-log-url-stats.html',
                collection_rows=result_iter(),
                count=num_urls, date=dt_string, days_ago=days_ago)


@app.route('/gae_stats/url_stats')
@auth.login_required
def gae_stats_url_stats():
    """This dashboard shows stats over time for a given URL."""
    url = flask.request.args.get('url', '/')
    # Get up to 3 years(ish) worth of data.
    url_stats = data.daily_request_log_url_stats(db, url=url, limit=1000)

    # Get a list of all the urls.  Some days this data isn't generated
    # properly, and some days it takes a while for yesterday's report
    # to be generated, so we just go back in time until we get a list
    # of urls; hopefully it's *fairly* up-to-date, at least.
    for days_ago in xrange(1, 8):
        dt_string = utc_as_dt(days_ago)
        urls = data.daily_request_log_url_stats(db, dt=dt_string,
                                                fields=['url'])
        urls = [u['url'] for u in urls]
        if urls:
            break
    else:
        urls = [url, '(Could not fetch full list of urls)']

    return flask.render_template('gae-stats/url-stats.html',
                                 current_url=url, urls=urls,
                                 url_stats=url_stats)


def _collect_records(records, fixed_keys, varying_key, varying_value):
    """Collate a list of records (each a dict) and return a new list.

    Input is a list of records, each of which is a list of dicts.
    The idea is to create new dicts which 'transpose' these records
    as follows:

    1) Collect together all records that have the same values for
    the fixed keys.  There may be many such records, which differ
    in the values of other keys.

    2) For a single collection, go through all the records and look
    for the values of the varying_key and varying_value for each
    record.  Add {dict[varying_key]: dict[varying_value]} to the
    record.

    As an example: fixed_keys = ('Year', 'Country').  varying_key =
    'City'.  varying_value = 'Population'.  Then if the records are
    [{'Year': 1972, 'Country': 'USA', 'City': 'NY', 'Population': 1000},
     {'Year': 1972, 'Country': 'USA', 'City': 'LA', 'Population': 500},
     {'Year': 1973, 'Country': 'USA', 'City': 'NY', 'Population': 2000},
     {'Year': 1973, 'Country': 'USA', 'City': 'Chicago', 'Population': 400},
     {'Year': 1973, 'Country': 'USA', 'City': 'LA', 'Population': 600},
     {'Year': 1972, 'Country': 'Canada', 'City': 'Toronto', 'Population': 6},
     ]
    The output would be:
    [{'Year': 1972, 'Country': 'USA', 'NY': 1000, 'LA': 500},
     {'Year': 1973, 'Country': 'USA', 'NY': 2000, 'LA': 600, 'Chicago': 400},
     {'Year': 1972, 'Country': 'Canada', 'Toronto': 6}
    ]

    Arguments:
      records: a list of dicts, such as is returned by mongodb.
      fixed_keys: a list/set of keys that we collect by.
      varying_key: a key of the input dict; its value is an output key.
      varying_value: a key of the input dict; its value is varying_key's value.

    Returns:
      A list of dicts, transposed as described above.
    """
    # First, collect together all records with the same values for fixed_keys
    collections = {}
    for record in records:
        collection_key = tuple((x, record[x]) for x in fixed_keys)
        collections.setdefault(collection_key, []).append(record)

    # Next, create the output record for each collection.
    retval = []
    for (fixed_keys_and_values, collection) in collections.iteritems():
        output_record = dict(fixed_keys_and_values)
        for record in collection:
            output_record[record[varying_key]] = record[varying_value]
        retval.append(output_record)

    return retval


@app.route('/webpagetest/stats')
@auth.login_required
def webpagetest_stats():
    """This dashboard shows download-speed over time for a given URL/etc."""
    # TODO(csilvers): get from analytics/src/webpagetest/run_webpagetest,
    # rather than cut-and-pasting them here.
    _BROWSER_LOCATIONS = (
        'Dulles_IE8',
        'Dulles_IE9',
        'Dulles:Chrome',
        'Dulles:Firefox',
        'SanJose_IE9',
        'London_IE8',
    )
    _URLS_TO_TEST = (
        'http://www.khanacademy.org/',
        'http://www.khanacademy.org/exercisedashboard',
        # An arbitrarily picked video
        ('http://khanacademy.org/math/algebra/solving-linear-equations'
         '/v/simple-equations'),
        # An arbitrarily picked exercise
        'http://www.khanacademy.org/math/calculus/e/derivative_intuition',
        # An arbitrarily picked CS scratchpad
        'http://www.khanacademy.org/cs/winston/823977317',
    )
    # Options here are DSL, Fios, Dial, and custom.
    _CONNECTIVITY_TYPES = (
        'DSL',
    )
    # These are the stats we chose to store in run_webpagetest.py.
    _STATS = (
        'Time to First Byte (ms)',
        'Time to Title',
        'Time to Base Page Complete (ms)',
        'Time to Start Render (ms)',
        'Doc Complete Time (ms)',
        'Load Time (ms)',
        'Activity Time(ms)',
        'Bytes In',
        'Bytes Out',
        'Requests',
        'DNS Lookups',
        )
    # These are the stats we graph by default
    _DEFAULT_STATS = ('Time to First Byte (ms)',
                      'Doc Complete Time (ms)',
                      )

    # These are the fields that are needed to select on, and by stats.html.
    input_fields = [
        'Date',
        'Browser Location',
        'URL',
        'Connectivity Type',
        'Cached',
        ]

    # We default to letting the user see all the stats from one url/location.
    browser_and_loc = flask.request.args.get('browser', _BROWSER_LOCATIONS[0])
    url = flask.request.args.get('url', _URLS_TO_TEST[0])
    connectivity = flask.request.args.get('connectivity',
                                          _CONNECTIVITY_TYPES[0])
    cached = flask.request.args.get('cached', '0')
    stat = flask.request.args.get('stat', _STATS[0])
    all_vars = (browser_and_loc, url, connectivity, cached, stat)

    # We want exactly one '(all)' field: that's the one that gets graphed.
    # If a field is '(new_all)', that one wins; it means the user just
    # selected it to be 'all'.  Otherwise, if there's more than one, we
    # pick arbitrarily.  If there are none, we make 'stats' the '(all)'.
    # TODO(csilvers): implement this.  But for now...
    if browser_and_loc == '(new_all)':
        browser_and_loc = '(all)'
    if url == '(new_all)':
        url = '(all)'
    if connectivity == '(new_all)':
        connectivity = '(all)'
    if cached == '(new_all)':
        cached = '(all)'
    if stat == '(new_all)':
        stat = '(all)'
    if '(all)' not in all_vars:
        stat = '(all)'

    # We filter on the given url/browser/etc, unless the user said
    # "(all)", in which case we don't filter on that field.  The way
    # we tell webpagetest_stats() not to filter is to pass in None.
    url_filter = url if url != '(all)' else None
    browser_filter = browser_and_loc if browser_and_loc != '(all)' else None
    connectivity_filter = connectivity if connectivity != '(all)' else None
    cached_filter = cached if cached != '(all)' else None
    stats_filter = [stat] if stat != '(all)' else list(_STATS)

    webpagetest_stats = data.webpagetest_stats(
        db,
        url=url_filter,
        browser=browser_filter,
        connectivity=connectivity_filter,
        cached=cached_filter,
        fields=(input_fields + stats_filter))

    # Now we need to collate the stats appropriately, depending on
    # which dimension is the '(all)' dimension.  If it's stats, then
    # we want each record to have all the stats (which it already
    # does, by default).  If it's url, we want each record to have
    # the single requested stat for all urls, which we'll have to
    # collate.  Etc.  This assumes that at most one dimension says
    # '(all)'.  If more than one does, we will give weird results.
    if stat == '(all)':
        collated_stats = webpagetest_stats[:]
        varying_fields = [{'name': s, 'default': s in _DEFAULT_STATS}
                          for s in _STATS]
    else:
        if url == '(all)':
            varying_key = 'URL'
            varying_fields = [{'name': u, 'default': True}
                              for u in _URLS_TO_TEST]
        elif browser_and_loc == '(all)':
            varying_key = 'Browser Location'
            varying_fields = [{'name': b, 'default': True}
                              for b in _BROWSER_LOCATIONS]
        elif connectivity == '(all)':
            varying_key = 'Connectivity'
            varying_fields = [{'name': ct, 'default': True}
                              for ct in _CONNECTIVITY_TYPES]
        elif cached == '(all)':
            varying_key = 'Cached'
            varying_fields = [{'name': c, 'default': True}
                              for c in (0, 1)]
        fixed_keys = input_fields[:]
        fixed_keys.remove(varying_key)
        collated_stats = _collect_records(webpagetest_stats, fixed_keys,
                                          varying_key, stat)

    # Now we need to fix up the date: convert 10/20/2012
    # to (2012, 9, 20) for use in the JavaScript Date constructor.
    for record in collated_stats:
        dt_parts = map(int, record['Date'].split('/'))
        record['Date'] = (dt_parts[2], dt_parts[0] - 1, dt_parts[1])

    return flask.render_template('webpagetest/stats.html',
                                 browser_locations=_BROWSER_LOCATIONS,
                                 current_browser_and_loc=browser_and_loc,
                                 urls=_URLS_TO_TEST,
                                 current_url=url,
                                 connectivities=_CONNECTIVITY_TYPES,
                                 current_connectivity=connectivity,
                                 current_cached=cached,
                                 stats=_STATS,
                                 current_stat=stat,
                                 fields=varying_fields,
                                 webpagetest_stats=collated_stats)


def utc_as_dt(days_ago=0):
    """Today's UTC date as a string dt for use in a mongo query.

    The 'dt' field stored in mongo on the analytics machine is in the format
    'YYY-MM-DD', and its clock is on UTC time.

    Arguments:
      days_ago: how many days ago to look at.  The default is today.
         Many callers will want yesterday (days_ago=1).
    """
    dt = datetime.datetime.utcnow() - datetime.timedelta(days_ago)
    return dt.strftime('%Y-%m-%d')


def main():
    parser = optparse.OptionParser()
    parser.add_option("-d", "--debug", action="store_true", dest="debug",
                      help="Whether to run in debug mode "
                           "(only accessible by localhost and autoreloads)")
    parser.add_option("-p", "--port", type="int", default=-1,
                      help="The port to run on (defaults to 5000 for debug, "
                           "else defaults to 80)")
    options, _ = parser.parse_args()

    app.debug = options.debug
    port = options.port
    if options.debug:
        if port == -1:
            port = 5000
        auth.configure_app(app, required=False)
        app.run(port=port)
    else:
        if port == -1:
            port = 80
        auth.configure_app(app, required=True)
        app.run(host='0.0.0.0', port=port)

if __name__ == '__main__':
    main()
