""" Library to massage data fetched from mongo report db"""


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


def _average_of_maps(maps):
    """Given a list of maps with the same keys, return an 'average' map.

    The return value has the same keys as the input maps, but for every
    value that's a number, the output value is the average of the
    value for all the input maps.  For every value that's not a number,
    the output value is taken arbitrarily from one of the input maps.
    """
    retval = {}
    for k in maps[0]:
        try:
            retval[k] = sum(m[k] for m in maps) / len(maps)
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
          If None, then average all the integer fields across all runs.
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
        entry['Run'] = '(average)'
        key = (entry.get('Date'),
               entry.get('URL'), entry.get('Browser Location'),
               entry.get('Connectivity Type'), entry.get('Cached'))
        entry_map.setdefault(key, []).append(entry)

    # Now each value in entry_map is a list of runs for the same url/etc.
    # We average every int value in there, taking the rest to be the
    # first.
    retval = []
    for key in sorted(entry_map.keys()):
        run = entry_map[key]
        retval.append(_average_of_maps(run))
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


def gae_usage_reports_for_resource(mongo, resource_name, limit=100):
    """Fetch from the mongo collection gae_dashboard_usage_reports.

    Arguments:
      mongo: a pymongo connection
      resource_name: the name of a resource from the App Engine usage
        report, e.g., "Frontend Instance Hours", "Datastore Storage".
      limit (optional): the maximum size of the result set. Default is 100.

    Returns:
      A generator of (dt, used, unit) tuples where dt is a date string like
      'YYYY-MM-DD', used is a numeric amount quantity (float or int), and
      unit is the App Engine unit such as "GByte-day".
    """
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
    for doc in cursor:
        for entry in doc['usage']:
            if entry['name'] == resource_name:
                yield doc['dt'], entry['used'], entry['unit']
                break  # break out of the usage items iteration


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
