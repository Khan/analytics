""" Library to massage data fetched from mongo report db"""

import collections
import datetime
import itertools


def topic_summary(mongo, dt, duration):
    """Get the topic summary based on dt and duration"""
    mongo_db = mongo['report']
    total_info = {}
    totals = mongo_db['video_stats'].find(
        {"dt": dt, "duration": duration, "aggregation": "total"})
    for db_row in totals:
        total_info = update_row(total_info, db_row)
    if len(total_info) == 0:
        return ([], [])

    top_results = get_keyed_results(mongo_db['video_stats'],
        total_info, 'title',
        {"dt": dt, "duration": duration, "aggregation": "top_topic"})
    second_results = get_keyed_results(mongo_db['video_stats'], total_info,
        'title',
        {"dt": dt, "duration": duration, "aggregation": "second_topic"})
    return (top_results, second_results)


def top_videos(mongo, dt, duration):
    """Get the top videos based on dt and duration"""
    db_collection = mongo['report']['video_stats']
    findCriteria = {"dt": dt, "duration": duration, "aggregation": "video"} 
    sortCriteria = [("seconds_watched", -1)]
    return get_keyed_results(db_collection, {}, 'title', findCriteria,
                             sortCriteria)


def video_title_summary(mongo, title, duration, start_dt, end_dt):
    """ Get the time series for a certain title"""
    if title == "":
        title = "Total"
    db_collection = mongo['report']['video_stats']
    criteria = {'duration': duration, 'title': title}
    dt = {}
    if start_dt:
        dt['$gte'] = start_dt
    if end_dt:
        dt['$lt'] = end_dt
    if dt:
        criteria.update({'dt': dt})
    return get_keyed_results(db_collection, {}, 'dt', criteria)


def daily_request_log_url_stats(mongo, dt=None, url=None, fields=None,
                                limit=100):
    """Fetch from the mongo collection daily_request_log_url_stats.

    Arguments:
      mongo: a pymongo connection.
      dt (optional): a date string like 'YYYY-MM-DD'. If specified, fetch only
        documents whose "dt" field matches.
      url (optional): fetch only documents whose "url" field matches.
      fields (optional): a list of field names to return in the result set.
      limit (optional): the maximum size of the result set. Default is 100.

    Returns:
      A list of dicts, each containing the fields specified in the "fields"
      argument, or all fields defined in the daily_request_log_url_stats table
      in map_reduce/hive/ka_hive_init.q
    """
    collection = mongo['report']['daily_request_log_url_stats']
    spec = {}
    if dt is not None:
        spec['dt'] = dt
    if url is not None:
        spec['url'] = url
    return collection.find(spec, fields).limit(limit)


def _median_of_maps(maps):
    """Given a list of maps with the same keys, return a 'median' map.

    The return value has the same keys as the input maps, but for every
    value that's a number, the output value is the median of the
    value for all the input maps.  For every value that's not a number,
    the output value is taken arbitrarily from one of the input maps.
    """
    retval = {}
    for k in maps[0]:
        try:
            values = [m[k] for m in maps]
            values.sort()
            if len(values) % 2:
                retval[k] = values[(len(values) - 1) / 2]
            else:
                retval[k] = (values[len(values) / 2] + 
                             values[len(values) / 2 - 1]) / 2
        except TypeError:    # m[k] is not a number
            retval[k] = maps[0][k]
    return retval


def webpagetest_stats(mongo, dt=None,
                      url=None, browser=None, connectivity=None, cached=None,
                      run=None,
                      fields=None, limit=100):
    """Fetch from the mongo collection webpagetest_reports.

    Arguments:
      mongo: a pymongo connection.
      dt (optional): a date string like 'YYYY-MM-DD'. If specified, fetch only
          documents from the given date (matching the "Date" field).
      url (optional): fetch only documents whose "URL" field matches.
          e.g. "http://www.khanacademy.org" or "/".  If the url does
          not have "://" in it, "http://www.khanacademy.org" is prepended.
      browser (optional): match "Browser Location" field.  e.g. "London_IE8".
      connectivity (optional): match "Connectivity Type" field.  e.g. "DSL".
      cached (optional): match "Cached" field.  If "0", the record does
          not use the browser cache (shift-reload).  If "1" it does (normal
          reload).
      run (optional): if specified, retrieve information for run #<run>.
          If None, then take the median for all the integer fields
          across all runs.
      fields (optional): a list of field names to return in the result set.
      limit (optional): the maximum size of the result set. Default is 100.

    Returns:
      A list of dicts, each containing the fields specified in the "fields"
      argument, or all fields listed in src/webpagetest/run_webpagetest.py:
      ConvertToDict().
    """
    collection = mongo['report']['webpagetest_reports']
    spec = {}
    if dt is not None:
        # Input is 2012-10-20, but webpagetest uses "10/20/2012".
        spec['Date'] = '%s/%s/%s' % (dt[5:7], dt[8:10], dt[0:4])
    if url is not None:
        if "://" not in url:
            url = 'http://www.khanacademy.org' + url
        spec['URL'] = url
    if browser is not None:
        spec['Browser Location'] = browser
    if connectivity is not None:
        spec['Connectivity Type'] = connectivity
    if cached is not None:
        spec['Cached'] = int(cached)
    if run is not None:
        spec['Run'] = int(run)

    if run is not None:   # easy case, no averaging
        return collection.find(spec, fields).limit(limit)

    # If we're going to average the runs, we need to fetch (limit *
    # #runs) results, since each collection of n runs becomes one
    # entry in the output.  But we don't know n!  We guess it's <= 3.
    wpt_data = collection.find(spec, fields).limit(limit * 3)

    entry_map = {}   # keys are (url, browser, connectivity, cached)
    for entry in wpt_data:
        entry['Run'] = '(median)'
        key = (entry.get('Date'),
               entry.get('URL'), entry.get('Browser Location'),
               entry.get('Connectivity Type'), entry.get('Cached'))
        entry_map.setdefault(key, []).append(entry)

    # Now each value in entry_map is a list of runs for the same
    # url/etc.  We take the median of every int value in there, taking
    # the rest to be the first.
    retval = []
    for key in sorted(entry_map.keys()):
        run = entry_map[key]
        retval.append(_median_of_maps(run))
    return retval[:limit]


def daily_request_log_urlroute_stats(mongo, dt, limit=100):
    """Fetch from the mongo collection daily_request_log_urlroute_stats.

    Arguments:
      mongo: a pymongo connection.
      dt: a date string like 'YYYY-MM-DD'. Fetch only documents whose "dt"
        field matches.
      limit (optional): the maximum size of the result set. Default is 100.

    Returns:
      A list of dicts, each containing the fields of the
      daily_request_log_urlroute_stats table in map_reduce/hive/ka_hive_init.q
    """
    collection = mongo['report']['daily_request_log_urlroute_stats']
    return collection.find({'dt': dt}).limit(limit)


def gae_dashboard_reports(mongo, report_name, limit=12 * 24 * 366):
    """Fetch reports from the mongo collection associated with a given
    report name.

    The collection is expected to have a "utc_datetime" field that
    represents when the report data was collected.

    Arguments:
      mongo: a pymongo connection.
      report_name: the name of the dashboard report. This is used to
        generate the collection name in the format "gae_dashboard_%s_reports"
        and must have a "utc_datetime" field
      limit (optional): the maximum size of the result set. Default is
        12*24*366 which is about a year.

    Returns:
      An iterable of documents from the report's collection in mongo,
      sorted from newest to oldest.
    """
    collection = mongo['report']['gae_dashboard_%s_reports' % report_name]
    cursor = collection.find(sort=[('utc_datetime', -1)], limit=limit)
    return iter(cursor)


def gae_usage_reports_for_resource(mongo, resource_name, limit=366,
                                   group_dt_by='day'):
    """Fetch from the mongo collection gae_dashboard_usage_reports.

    Arguments:
      mongo: a pymongo connection
      resource_name: the name of a resource from the App Engine usage
        report, e.g., "Frontend Instance Hours", "Datastore Storage".
      limit (optional): the maximum size of the result set. Default is
        366, or about a year.
      group_dt_by (optional): expects either "day" or "week". Normally
        each result's "used" field is the amount for a single day.
        Specifying "week" means that the result's "dt" field is the
        Monday of that week and the "used" field is the sum for the days
        in the week. Incomplete weeks (those with less than 7 days) are
        ignored. 

    Returns:
      The tuple (result_iterator, unit) where unit is the App Engine
      unit such as "GByte-day" and result_iterator returns dicts like
      {'date': datetime.date, 'amount_of_resource_used': ...} where used
      is a numeric amount quantity (float or int).  When there are no
      results, unit is the empty string.
    """
    if not group_dt_by in ('day', 'week'):
        raise ValueError('group_dt_by must be one of "day", "week"')
    collection = mongo['report']['gae_dashboard_usage_reports']
    cursor = collection.find(sort=[('dt', -1)], limit=limit)
    # Each document has the following structure, where usage entries
    # match the data from the billing history CSV published by App Engine:
    #
    # {'dt': '2012-10-15',
    #  'usage': [
    #    {'name': 'Frontend Instance Hours', 'unit': 'Hour', 'used': XX.XX},
    #    {'name', 'Datastore Storage', 'unit': 'GByte-day', 'used': YY.YY},
    #    ... ]}
    try:
        peek = cursor[0]
        cursor = itertools.chain([peek], cursor)
    except IndexError:
        # No items returned by this cursor.
        peek = None

    unit = ''
    if peek:
        # Extract a resource's unit of usage or fall back to the empty
        # string. This function is expected to normalize units though in
        # practice the unit remains uniform. A resource billed by "Day"
        # will always bill by "Day" and not in "Hour". This may need
        # revision if App Engine changes how units are billed.
        for entry in peek['usage']:
            if entry['name'] == resource_name:
                unit = entry['unit']
                break

    def result_iter():
        for doc in cursor:
            for entry in doc['usage']:
                if entry['name'] == resource_name:
                    date = datetime.date(*map(int, doc['dt'].split('-')))
                    yield {'date': date,
                           'amount_of_resource_used': entry['used']}
                    break  # break out of the usage items iteration

    if group_dt_by == 'day':
        return result_iter(), unit
    else:
        # Sum daily resource usage into weeks that start on Monday.
        daily = list(result_iter())
        week_buckets = collections.OrderedDict()
        for date, used in daily:
            date -= datetime.timedelta(date.weekday())  # get to Monday
            monday = '%d-%d-%d' % (date.year, date.month, date.day)
            if monday not in week_buckets:
                week_buckets[monday] = []
            week_buckets[monday].append(used)
        weekly_iter = ({'date': monday,
                        'amount_of_resource_used': sum(used_list)}
                       for monday, used_list in week_buckets.iteritems())
        return weekly_iter, unit


def get_keyed_results(db_collection, total_info, index_key, 
                      findCriteria, sortCriteria=None):
    """Get the video statistics by index_key based on db criteria"""
    results = {}
    db_results = db_collection.find(findCriteria, sort=sortCriteria)
    for db_row in db_results:
        title = db_row[index_key]
        results[title] = update_row(results.get(title, {}), db_row)
    if total_info:
        results['Total'] = {}
        results['Total'].update(total_info)
    # Calculating percentages and serialize them into row
    rows = []
    for title, result in results.iteritems():
        pct_result = {}
        for key, value in result.iteritems():
            if total_info.get(key, 0):
                pct_result[key + "_pct"] = float(value) / total_info[key]
            else:
                pct_result[key + "_pct"] = 0
        result.update(pct_result)
        result[index_key] = title
        if title != 'NONE':
            rows.append(result)
    return rows


def update_row(row, db_row, total_info=None):
    """Update a row in the result based on a db row"""
    suffix = "_" + db_row['user_category']
    row["watched" + suffix] = db_row["users"]
    row["visits" + suffix] = db_row["visits"]
    row["completed" + suffix] = db_row["completed"]
    row["hours" + suffix] = float("%.2f" % (
        float(db_row["seconds_watched"]) / 3600))
    row["visits_per_user" + suffix] = float("%.2f" % (
        float(db_row["visits"]) / db_row["users"]))
    return row
