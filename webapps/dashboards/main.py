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
                instance_counts.append((tuple(map(int, timestamp_tuple[0])),
                                        len(report)))
    return flask.render_template('gae-stats/instances.html',
                                 instance_counts=instance_counts)


@app.route('/gae_stats/daily_request_log_url_stats')
@auth.login_required
def gae_stats_daily_request_log_url_stats():
    """This dashboard shows stats for the most accessed URLs."""
    results = data.daily_request_log_url_stats(db, dt=yesterday_utc_as_dt())
    return flask.render_template('gae-stats/daily-request-log-url-stats.html',
                                 collection_rows=results)


@app.route('/gae_stats/daily_request_log_urlroute_stats')
@auth.login_required
def gae_stats_daily_request_log_urlroute_stats():
    """This dashboard shows stats for the most accessed URLs, grouped by the
    route patterns that they match for handlers on the website.
    """
    results = data.daily_request_log_urlroute_stats(db, yesterday_utc_as_dt())

    def result_iter():
        # Set 'url' so that we can reuse the same template as
        # daily_request_log_url_stats.  This is done one-by-one in a generator
        # and not by iterating over the results here in order to avoid
        # exhausting the "results" cursor.
        for row in results:
            row['url'] = row['url_route']
            yield row

    return flask.render_template('gae-stats/daily-request-log-url-stats.html',
                                 collection_rows=result_iter())


@app.route('/gae_stats/url_stats')
@auth.login_required
def gae_stats_url_stats():
    """This dashboard shows stats over time for a given URL."""
    url = flask.request.args.get('url', '/')

    url_stats = data.daily_request_log_url_stats(db, url=url)
    urls = data.daily_request_log_url_stats(db, dt=yesterday_utc_as_dt(),
                                            fields=['url'])

    urls = [u['url'] for u in urls]
    return flask.render_template('gae-stats/url-stats.html',
                                 current_url=url, urls=urls,
                                 url_stats=url_stats)


def yesterday_utc_as_dt():
    """Yesterday's UTC date as a string dt for use in a mongo query.

    The 'dt' field stored in mongo on the analytics machine is in the format
    'YYY-MM-DD', and its clock is on UTC time.
    """
    yesterday = datetime.datetime.utcnow() - datetime.timedelta(1)
    return yesterday.strftime('%Y-%m-%d')


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
