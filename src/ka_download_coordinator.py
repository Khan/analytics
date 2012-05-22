"""Library to monitor and coordinate the fetch and loading of data
 store entities from GAE.  Here is roughly how things work here. 
We will the collections under the control_db
    1. current_report: record the next start_date for each kind. If no kind 
        is found, start with (now - config['default_start_time']) in seconds 
    2. gaps: records failures and gaps need to be refilled
    3. ProgressLogs: the actual downloading and loading status
    4. monitor_report: record the monitored progress for each kind. Timestamps
         record here minus the gaps are the data available
    5. archived_logs: store the old, successfull records 
"""
import datetime as dt
import re

from util import db_decorator

class DownloadStatus: 
    NONE = 0
    STARTED = 1
    FETCHED = 2 
    SAVED = 3
    LOADED = 4 # == SUCCESS
    SUCCESS = 4

def get_current_report(mongo, config):
    def _get_current_report(mongo, config, kind, start_dt, end_dt, status):
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
    func = db_decorator(5, _record_progress)
    func(mongo, config, kind, start_dt, end_dt, status)

def set_current_report(config): 
    pass

def get_key(kind, start_dt, end_dt):
    key = "%s%s%s" % (kind, start_dt, end_dt)
    return re.sub(r'[^a-zA-Z0-9]', '', key)
    
def record_progress(mongo, config, kind, start_dt, end_dt, status):
    """Record the downloading progress""" 
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
    func = db_decorator(5, _record_progress)
    func(mongo, config, kind, start_dt, end_dt, status)
            
