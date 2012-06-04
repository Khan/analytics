"""Library to monitor and coordinate the loading of datastore entities from GAE.
The record_progress() function records the GAE download progress.
The get_failed_jobs() function gets the failed download tasks for reprocessing.
"""
import datetime as dt
import re

from util import db_decorator


class DownloadStatus:
    NONE = 0
    STARTED = 1
    FETCHED = 2
    SAVED = 3
    LOADED = 4  # == SUCCESS
    SUCCESS = 4


def get_failed_jobs(mongo, config):
    """Get gae download tasks with status != SUCCESS."""
    def _get_failed_jobs(mongo, config):
        mongo_db = mongo[config['control_db']]
        mongo_collection = mongo_db['ProgressLogs']
        query = {"status": {"$lt": DownloadStatus.SUCCESS}}
        return mongo_collection.find(query)
    func = db_decorator(5, _get_failed_jobs)
    return func(mongo, config)


def get_key(kind, start_dt, end_dt):
    key = "%s%s%s" % (kind, start_dt, end_dt)
    return re.sub(r'[^a-zA-Z0-9]', '', key)


def record_progress(mongo, config, kind, start_dt, end_dt, status):
    """Mark the downloading status of entity kind with backup timestamp
       between start_dt and end_dt. NOTE: We download the data incrementally.
       That's why the status is marked with (kind, start_dt, end_dt) tuples.
       Arguments:
         mongo: mongo connection
         config: the donwload control db config
         kind: datastore entity type
         start_dt, end_dt: backup_timestamp range of the entity type
         status: one of the enum values in DownloadStatus
    """
    def _record_progress(mongo, config, kind, start_dt, end_dt, status):
        key = get_key(kind, start_dt, end_dt)
        mongo_db = mongo[config['control_db']]
        mongo_collection = mongo_db['ProgressLogs']
        db_doc = mongo_collection.find_one(key)
        history = {}
        if db_doc and 'history' in db_doc:
            history = db_doc['history']
        history[str(status)] = dt.datetime.now()
        doc = {'_id': key, 'kind': kind, 'start_dt': start_dt,
               'end_dt': end_dt, 'status': status, 'history': history}
        mongo_collection.save(doc)
    func = db_decorator(max_tries=5, func=_record_progress)
    func(mongo, config, kind, start_dt, end_dt, status)
