""" Library to massage data fetched from mongo report db"""
import pymongo


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


def get_keyed_results(db_collection, total_info, index_key, criteria):
    """Get the video statistics by index_key based on db criteria"""
    results = {}
    db_results = db_collection.find(criteria)
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
        db_row["seconds_watched"] / 3600.0))
    row["visits_per_user" + suffix] = float("%.2f" % (
        float(db_row["visits"]) / db_row["users"]))
    return row
