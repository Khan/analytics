#!/usr/bin/env python

"""A dashboard webapp.

To run the dashboards web app in debug mode on port 5000:

   ./main.py -d


This hosts the following dashboards:
- daily exercise statistics over time
- a daily report of videos watched
- learning efficiency and retention from exercises
- performance-related dashboards

It will house more dashboards for fundamental metrics we want to track.
"""

import collections
import datetime
import json
import operator
import optparse
import os
import time

import flask
import pymongo

import auth
import data
import google_analytics
import utf8csv

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


@app.route('/exercise-summary')
@auth.login_required
def exercise_summary_dashboard():
    return flask.render_template('exercise-summary.html')


@app.route('/teachers-students')
@auth.login_required
def teacher_student_dashboard():
    return flask.render_template('teacher-student-count.html')


@app.route('/badges')
@auth.login_required
def badges_dashboard():
    return flask.render_template('badge_summary.html')


@app.route('/cover-graph')
@auth.login_required
def cover_graph():
    return flask.render_template('cover_graph.html')


# Serve map data as a csv file. Generate one file per week
@app.route("/teachers-students/download-map")
@auth.login_required
def download_teacher_map():
    # Due to discrepancies with handling relative paths by flask and
    # python os module we have to make paths absolute to avoid errors
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "geo")
    if not os.path.exists(path):
        os.mkdir(path)

    now = datetime.datetime.now()
    most_recent_monday = now.replace(day=now.day - now.weekday(),
        hour=0, minute=0, second=0, microsecond=0)

    # Changing file name prefix may impact which files will be deleted below.
    # When in doubt delete old files since they are auto generated
    filename_str = "teacher_geo_{0}.csv"
    date_format = "%Y-%m-%d"

    filename = filename_str.format(most_recent_monday.strftime(date_format))
    file_path = os.path.join(path, filename)

    geo_data = db.report.teacher_country.find({}, {"_id": 0})
    with open(file_path, "wb") as f:

        description = ["Teacher ID", "Teacher Email", "Teacher Nickname",
                       "City", "Region", "Country", "Country Code",
                       "Longitude", "Latitude"]

        csv_writer = utf8csv.UnicodeWriter(f)
        csv_writer.writerow(description)

        for db_record in geo_data:
            row = [db_record["user_id"],
                   db_record["user_email"],
                   db_record["user_nickname"],
                   db_record["city"],
                   db_record["region"],
                   db_record["country"],
                   db_record["country_code"],
                   str(db_record["longitude"]),
                   str(db_record["latitude"])]

            csv_writer.writerow(row)

    # This is only correct when lexicographical order of files is the desired
    #  one.
    # If some files have the same date (in name) it does not matter which one
    #  will be removed.
    files_with_newest_first = sorted(os.listdir(path), reverse=True)
    for name in files_with_newest_first[1:]:
        os.unlink(os.path.join(path, name))

    return flask.send_from_directory(path, filename, as_attachment=True)


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


@app.route('/data/exercise-summary/all')
@auth.login_required
def exercise_summary_top():
    start_dt = flask.request.args.get('start_date')
    end_dt = flask.request.args.get('end_date')
    all_ex = data.exercise_summary(db, start_dt, end_dt)
    return flask.jsonify({
        "exercise_data": all_ex
    })


@app.route('/data/exercise-summary/<exercise>')
@auth.login_required
def exercise_summary(exercise):
    start_dt = flask.request.args.get('start_date')
    end_dt = flask.request.args.get('end_date')
    sub_exercise_type = flask.request.args.get('sub_exercise_type')
    exercise_data = data.exercise_summary(db, start_dt, end_dt,
                                exercise, sub_exercise_type)
    return flask.jsonify({
        "exercise_data": exercise_data
    })


@app.route('/data/exercise-proficiency-summary/all')
@auth.login_required
def exercise_proficiency_summary_all():
    proficiency = data.proficiency_summary(db)
    return flask.jsonify({
        "proficiency_data": proficiency
    })


@app.route('/data/exercise-proficiency-summary/<exercise>')
@auth.login_required
def exercise_proficiency_summary(exercise):
    proficiency = data.proficiency_summary(db, exercise)
    return flask.jsonify({
        "proficiency_data": proficiency[0] if len(proficiency) else {}
    })


@app.route('/data/badge-summary/all')
@auth.login_required
def all_badge_summary():
    start_dt = flask.request.args.get('start_date')
    end_dt = flask.request.args.get('end_date')
    badges = data.badge_summary(db.report.badge_summary,
        start_dt, end_dt)
    return flask.jsonify({
        "badges": badges
    })


@app.route('/data/badge-summary/<badge>')
@auth.login_required
def badge_summary(badge):
    start_dt = flask.request.args.get('start_date')
    end_dt = flask.request.args.get('end_date')
    context_name = flask.request.args.get('context_name')
    badges = data.badge_summary(db.report.badge_context_summary,
        start_dt, end_dt, badge, context_name)
    return flask.jsonify({
        "badges": badges
    })


@app.route("/data/statusboard/company-goals")
def statusboard_company_goals():
    """Return company goals data formatted for http://panic.com/statusboard/"""
    collection = db["report"]["company_metrics"]
    results = collection.find(sort=[("activity_month", 1)])
    long_term_users = []
    registered_users = []
    highly_engaged_users = []
    for result in results:
        dt = result["activity_month"]
        long_term_users.append({
            "title": dt,
            "value": result["long_term_users_active_this_month"]
        })
        registered_users.append({
            "title": dt,
            "value": result["registrations_this_month"]
        })
        highly_engaged_users.append({
            "title": dt,
            "value": result["highly_engaged_users_active_this_month"]
        })
    return flask.jsonify({
        "graph": {
            "title": "Company Growth Metrics",
            "type": "line",
            "datasequences": [
                {
                    "title": "Long term users",
                    "color": "pink",
                    "datapoints": long_term_users[:-1]
                },
                {
                    "title": "Highly engaged users",
                    "color": "green",
                    "datapoints": highly_engaged_users[:-1]
                },
                {
                    "title": "Registered users",
                    "color": "orange",
                    "datapoints": registered_users[:-1]
                }
            ]
        }
    })


@app.route("/data/statusboard/exercises")
def statusboard_exercises():
    """Return exercise data formatted for http://panic.com/statusboard/"""
    collection = db["report"]["daily_exercise_stats"]
    criteria = {
        "exercise": "ALL",
        "sub_mode": "everything",
        "super_mode": "everything"
    }
    results = collection.find(criteria, sort=[("dt", -1)])
    exercises = []
    for result in results[:28]:
        exercises.append({
            "title": result["dt"],
            "value": result["problems"]
        })
    exercises.reverse()
    return flask.jsonify({
        "graph": {
            "title": "Exercises",
            "type": "bar",
            "total": True,
            "datasequences": [
                {
                    "title": "Problems per day",
                    "color": "yellow",
                    "datapoints": exercises
                }
            ]
        }
    })


@app.route("/data/statusboard/videos")
def statusboard_videos():
    """Return video data formatted for http://panic.com/statusboard/"""
    results = sorted(
                data.video_title_summary(db, "Total", "day", None, None),
                key=operator.itemgetter("dt"),
                reverse=True)
    videos = []
    for result in results[:28]:
        videos.append({
            "title": result["dt"],
            "value": int(result["hours_all"] * 60)
        })
    videos.reverse()
    return flask.jsonify({
        "graph": {
            "title": "Videos",
            "type": "bar",
            "total": True,
            "datasequences": [
                {
                    "title": "Minutes viewed per day",
                    "color": "blue",
                    "datapoints": videos
                }
            ]
        }
    })


@app.route("/data/statusboard/exercises/today")
def statusboard_exercises_today_value():
    """Return data for the exercise widget for http://panic.com/statusboard/"""
    collection = db["report"]["daily_exercise_stats"]
    criteria = {
        "exercise": "ALL",
        "sub_mode": "everything",
        "super_mode": "everything"
    }
    results = collection.find(criteria, sort=[("dt", -1)])
    week_ago = datetime.date.today() - datetime.timedelta(days=7)
    week_ago_str = week_ago.strftime("%Y-%m-%d")
    for result in results[:7]:
        if result["dt"] == week_ago_str:
            return flask.jsonify({"value": result["problems"]})
    # TODO(dylan): Do something better than fudging the value
    return flask.jsonify({"value": 1500000})


@app.route("/data/statusboard/videos/today")
def statusboard_videos_today_value():
    """Return data for the video widget for http://panic.com/statusboard/"""
    start_dt = datetime.date.today() - datetime.timedelta(days=7)
    end_dt = start_dt + datetime.timedelta(days=1)
    start_dt_str = start_dt.strftime("%Y-%m-%d")
    end_dt_str = end_dt.strftime("%Y-%m-%d")
    summary = data.video_title_summary(
        db, "Total", "day", start_dt_str, end_dt_str)
    if len(summary) == 0:
        # TODO(dylan): Do something better than fudging the value
        value = 20000 * 60
    else:
        value = summary[0]["hours_all"] * 60
    return flask.jsonify({"value": value})


@app.route('/db/exercise-summary/<exercise>/sub_types')
@auth.login_required
def exercise_summary_problem_type(exercise):
    types = db.report.exercise_summary.find({
        "exercise": exercise
    }).distinct('sub_exercise_type')
    return flask.jsonify({
        "sub_exercise_type": types
    })


@app.route('/db/exercise-summary/date-ranges')
@auth.login_required
def exercise_summary_dates():
    dates = db.report.exercise_summary.distinct('dt')
    return flask.jsonify({
        "dates": dates
    })


@app.route('/db/exercise-summary/exercises')
@auth.login_required
def exercise_summary_exercises():
    exercises = db.report.exercise_summary.distinct('exercise')
    return flask.jsonify({
        "exercises": exercises
    })


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
    group_dt_by = flask.request.args.get('group_dt_by', None)
    if group_dt_by not in ('day', 'week'):
        group_dt_by = 'day'

    resource_name = flask.request.args.get('resource', None)
    if resource_name not in _billing_resources:
        resource_name = _billing_resources[0]

    result_iter, resource_unit = data.gae_usage_reports_for_resource(
        db, resource_name, group_dt_by=group_dt_by)
    date_record_pairs = time_series(result_iter, 'date')

    return flask.render_template('gae-stats/billing-history.html',
                                 resource_name=resource_name,
                                 resource_unit=resource_unit,
                                 resources=_billing_resources,
                                 group_dt_by=group_dt_by,
                                 date_record_pairs=date_record_pairs)


# Entries have the same order as on the App Engine dashboard and map
# to either a string (the field name of the only series in the graph)
# or a dict that maps series labels to field names.
_dashboard_chart_fields = collections.OrderedDict([
    ('Requests/Second', 'requests_per_second'),
    ('Requests by Type/Second', {
        'Static Requests': 'static_requests_per_second',
        'Dynamic Requests': 'dynamic_requests_per_second',
        'Cached Requests': 'cached_requests_per_second',
        'PageSpeed Requests': 'pagespeed_requests_per_second',
        }),
    ('Milliseconds/Request', {
        'Dynamic Requests': 'milliseconds_per_dynamic_request',
        }),
    ('Errors/Second', 'errors_per_second'),
    ('Bytes Received/Second', 'bytes_received_per_second'),
    ('Bytes Sent/Second', 'bytes_sent_per_second'),
    ('CPU Seconds Used/Second', {
        'Total CPU': 'total_cpu_seconds_used_per_second',
        'API Calls CPU': 'api_cpu_seconds_used_per_second',
        }),
    ('Milliseconds Used/Second', 'milliseconds_used_per_second'),
    ('Number of Quota Denials/Second', {
        'Quota Denials': 'quota_denials_per_second',
        'DOS API Denials': 'dos_api_denials_per_second',
        }),
    ('Instances', {
        'Total': 'total_instance_count',
        'Active': 'active_instance_count',
        'Billed': 'billed_instance_count',
        }),
    ('Memory Usage (MB)', 'memory_usage_mb'),
    ])


@app.route('/gae_stats/dashboard_charts')
@auth.login_required
def gae_stats_dashboard_charts():
    chart_name = flask.request.args.get('chart', 'Requests/Second')
    if chart_name not in _dashboard_chart_fields:
        return flask.current_app.response_class(
            'Unrecognized chart name: "%s"' % chart_name, status=400)

    if isinstance(_dashboard_chart_fields[chart_name], basestring):
        chart_fields = [_dashboard_chart_fields[chart_name]]
    else:
        chart_fields = _dashboard_chart_fields[chart_name].values()
    records = data.gae_dashboard_reports(db, 'chart')
    date_record_pairs = time_series(records, 'utc_datetime')
    # TODO(chris): add support for grouping metrics to
    # gae-stats/metrics-in-time-series.html and then use that.
    return flask.render_template('gae-stats/dashboard-charts.html',
                                 title='Dashboard Charts',
                                 date_record_pairs=date_record_pairs,
                                 record_fields=chart_fields,
                                 chart_names=_dashboard_chart_fields.keys(),
                                 selected_chart=chart_name)


@app.route('/gae_stats/instances')
@auth.login_required
def gae_stats_instances():
    selected_metric = flask.request.args.get('metric', 'num_instances')
    # The tuples are (database_field_name, friendly_label).
    metrics = (('num_instances', 'number of instances'),
               ('average_qps', 'average qps'),
               ('average_latency_ms', 'average latency (ms)'),
               ('average_memory_mb', 'average memory (MB)'))
    records = data.gae_dashboard_reports(db, 'instance')
    date_record_pairs = time_series(records, 'utc_datetime')
    return flask.render_template('gae-stats/metrics-in-time-series.html',
                                 title='Instance Usage',
                                 date_record_pairs=date_record_pairs,
                                 metrics=metrics,
                                 selected_metric=selected_metric)


@app.route('/gae_stats/memcache')
@auth.login_required
def gae_stats_memcache():
    selected_metric = flask.request.args.get('metric', 'hit_ratio')
    # The tuples are (database_field_name, friendly_label).
    metrics = (('hit_ratio', 'hit ratio'),
               ('hit_count', 'hit count'),
               ('miss_count', 'miss count'),
               ('item_count', 'item count'),
               ('total_cache_size_bytes', 'total cache size (bytes)'),
               ('oldest_item_age_seconds', 'oldest item age (seconds)'))
    records = data.gae_dashboard_reports(db, 'memcache')
    date_record_pairs = time_series(records, 'utc_datetime')
    return flask.render_template('gae-stats/metrics-in-time-series.html',
                                 title='Memcache Statistics',
                                 date_record_pairs=date_record_pairs,
                                 metrics=metrics,
                                 selected_metric=selected_metric)


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
    date_record_pairs = time_series(url_stats, 'dt', '%Y-%m-%d')

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
                                 date_record_pairs=date_record_pairs)


@app.route('/gae_stats/urlroute_stats')
@auth.login_required
def gae_stats_urlroute_stats():
    """This dashboard shows stats over time for a given URL."""
    # The URL route is passed in the "url" parameter because we're
    # reusing the url_stats.html template, which does deal in URLs.
    # Valid values are generated by
    # map_reduce/py/raw_log_to_request_log_mapper.py's route_for_url()
    # method, and look like "<app.yaml module>:<handler regexp>", where
    # the module and handlers come from the main website.
    url_route = flask.request.args.get('url', 'main:/')
    # Get up to 3 years(ish) worth of data.
    url_stats = data.daily_request_log_urlroute_stats(db, url_route=url_route,
                                                      limit=1000)
    date_record_pairs = time_series(url_stats, 'dt', '%Y-%m-%d')

    def result_iter():
        # Set 'url' so that we can reuse the same template as
        # url_stats.  This is done one-by-one in a generator and not by
        # iterating over the results here in order to avoid exhausting
        # the "results" cursor.
        for date, record in date_record_pairs:
            record['url'] = record['url_route']
            yield date, record

    # Get a list of all the routes.  Some days this data isn't generated
    # properly, and some days it takes a while for yesterday's report to
    # be generated, so we just go back in time until we get a list of
    # routes; hopefully it's *fairly* up-to-date, at least.
    for days_ago in xrange(1, 8):
        dt_string = utc_as_dt(days_ago)
        routes = data.daily_request_log_urlroute_stats(db, dt=dt_string,
                                                       fields=['url_route'])
        routes = [r['url_route'] for r in routes]
        if routes:
            break
    else:
        routes = [url_route, '(Could not fetch full list of urls)']

    return flask.render_template('gae-stats/url-stats.html',
                                 current_url=url_route, urls=routes,
                                 date_record_pairs=result_iter())


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


@app.route("/statusboard/exercises/today")
def statusboard_exercises_today_widget():
    """Return an exercise widget for http://panic.com/statusboard/"""
    return flask.render_template(
                "statusboard-counter-widget.html",
                title="Exercises completed today",
                value="",
                source="/data/statusboard/exercises/today"
            )


@app.route("/statusboard/videos/today")
def statusboard_videos_today_widget():
    """Return a video widget for http://panic.com/statusboard/"""
    return flask.render_template(
                "statusboard-counter-widget.html",
                title="Minutes of video watched today",
                value="",
                source="/data/statusboard/videos/today"
            )


@app.route("/statusboard/stories")
def statusboard_stories_widget():
    """Return a story widget for http://panic.com/statusboard/"""
    return flask.render_template(
        "statusboard-story-widget.html",
        title="Stories",
        value="",
        source="http://www.khanacademy.org/api/internal/stories?count=100"
    )


@app.route("/is-ka-fast-yet")
@auth.login_required
def is_ka_fast_yet():
    """Key performance metrics for speeding up the homepage in Q1 2014."""
    start_date = flask.request.args.get("start_date", "2014-01-01")
    end_date = flask.request.args.get("end_date",
                                      datetime.date.today().isoformat())
    analytics_service = google_analytics.initialize_service()
    # Build your queries at http://ga-dev-tools.appspot.com/explorer/
    response = analytics_service.data().ga().get(
        ids="ga:13634188",  # GA view ID for www.khanacademy.org
        start_date=start_date,
        end_date=end_date,
        dimensions="ga:date,ga:customVarValue1",
        metrics="ga:avgDomContentLoadedTime,ga:domLatencyMetricsSample",
        filters="ga:pagePath==/;ga:country==United States"
    ).execute()
    return flask.render_template("is-ka-fast-yet.html",
                                 jsonStr=json.dumps(response))


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


def datetime_as_js_date(dt, strptime_format=None):
    """Format a date and time as a JavaScript Date constructor.

    Python's datetime class indexes months from 1.  JavaScript starts from 0.

    Arguments:
      dt: the date and time to convert.  datetime.datetime instances and
        strings matching "strptime_format" are valid inputs.  Note that
        if dt is a datetime.datetime then case tzinfo and microseconds
        are ignored.
      strptime_format: when "dt" is a string it will be parsed by
        time.strptime(dt, strptime_format)[0:6].

    Returns:
      The corresponding JavaScript Date constructor string.  For
      example, when str(dt) is "2012-12-05 14:22:00" this returns
      "Date(2012, 11, 5, 14, 22, 00)".
    """
    if isinstance(dt, datetime.datetime):
        args = (dt.year, dt.month - 1, dt.day, dt.hour, dt.minute, dt.second)
    elif isinstance(dt, datetime.date):
        args = (dt.year, dt.month - 1, dt.day)
    elif isinstance(dt, basestring):
        t = time.strptime(dt, strptime_format)
        args = (t.tm_year, t.tm_mon - 1, t.tm_mday,
                t.tm_hour, t.tm_min, t.tm_sec)
        if args[3:] == (0, 0, 0):
            args = args[:3]  # they just want the day
    else:
        raise ValueError('Unable to convert %s' % type(dt))
    return 'Date%s' % str(args)


def time_series(records, time_index, strptime_format=None):
    """Generate a standard format from records with a time field.

    Arguments:
      records: ordered time series data.
      time_index: the key used to index into each record to access its
        date field.  Each record should respond to record[time_key] with
        a value that datetime_as_js_date() can understand.
      strptime_format (Optional): passed along to datetime_as_js_date().

    Returns:
      (javascript_date, record) tuples where javascript_date is a string
      like "Date(2012, 3, 4, 8, 30, 0)".
    """
    for record in records:
        js_date = datetime_as_js_date(record[time_index], strptime_format)
        yield js_date, record


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
