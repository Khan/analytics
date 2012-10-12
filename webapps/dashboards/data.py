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


def daily_request_log_url_stats(mongo, dt=None, url=None, fields=None, limit=100):
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
