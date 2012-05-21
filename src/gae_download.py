#!/usr/bin/env python
"""Script to download data from the Google App Engine Data Store.  It takes a 
json config file to specify what entity types to download and the detailed 
download configurations.  The program also takes the start_date and end_date
 to specify the data duration we would like to download.  All the entity types
 have to have the field "backup_timestamp" to be downloaded properly. 
The entity keys becomes the _id in the mongo db. 
See the config under ../../cfg for more details

TODO(yunfang): adding a controldb in mongo to coordinate the fetch and 
               db load of gae data
"""

import datetime as dt
import errno
import json
import logging
import os
import pickle
import re
import subprocess
import sys 
import time

from optparse import OptionParser
from multiprocessing import Process, active_children
  
import pymongo 
from pymongo.errors import (InvalidDocument, DuplicateKeyError, 
                            InvalidStringData, AutoReconnect)

from google.appengine.ext import db
from google.appengine.datastore import entity_pb
from google.appengine.api import datastore, datastore_types, users

import fetch_entities 
from ka_util import (mkdir_p, load_unstripped_json,
                     get_logger, db_decorator)
import ka_download_coordinator as kdc

DEFAULT_DOWNLOAD_SETTINGS = {  
    "max_threads": 4, # max number of parrellel threads
    "max_tries": 8, # max number of tries to download entities
    "interval": 120, # data accumulated before writing into mongodb
    "sub_process_time_out": 1800, #sub process timeout in seconds
    "max_logs": 1000, # max number of entities from gae foreach pbuf call
    "dbhost": "localhost",
    "dbport": 28017,
    "default_db": "testdb", #dbname to write to
    "archive_dir": "archive"
}

COLLECTION_INDICES = {
    'UserData' : ['user', 'current_user', 'user_email'],
    'UserExercise' : [[('user',1), ('exercise',1)]],
    'UserVideo' : ['user'], 
    'VideoLog' : ['user', 'video', 
                  [('backup_timestamp', -1), ('user', 1), ('video', 1)]], 
    'ProblemLog' : ['user', 'exercise',
                    [('backup_timestamp', -1), ('user', 1), ('exercise', 1)]]
}

g_logger = get_logger()

def get_cmd_line_args():
    today_dt = dt.datetime.combine(dt.date.today(), dt.time())
    yesterday_dt = today_dt - dt.timedelta(days=1)

    parser = OptionParser(usage="%prog [options]",
        description="Download data from the Google App Engine Datastore")
    parser.add_option("-c", "--config", 
        help="json config file for all the download details")
    parser.add_option("-s", "--start_date",
        help="Earliest inclusive date of logs to fetch, in ISO 8601 format. \
              Defaults to yesterday at 00:00.")
    parser.add_option("-e", "--end_date",
        help="Latest exclusive date of logs to fetch, in ISO 8601 format. \
              Defaults to today at 00:00.")
    parser.add_option("-r", "--redo", default = 0,
        help="Re-fetch and overwrite db entries. default to 0. ")
    parser.add_option("-t", "--test", default = 0,
        help="test mode. write data to test_archive_dir instead")
    parser.add_option("-p", "--proc_interval", default = 3600,
        help="process interval if no start_date end_date specified")
    
    options, extra_args = parser.parse_args()
    if not options.config:
        g_logger.fatal('Please specify the json config file')
        exit(1)
    return options

def get_archive_file_name(config, kind, start_dt, end_dt): 
    """get the archive file name. has the format of 
       {ARCHIVE_DIR}/YY-mm-dd/{kind}/kind-start_dt-end_dt.pickle   
    """
    datestr = str(start_dt.date())
    dirname = "%s/%s/%s" % (config['archive_dir'], datestr, kind)
    mkdir_p(dirname)
    filename = "%s/%s-%s-%s-%s-%s.pickle" % (dirname, kind, 
        str(start_dt.date()), str(start_dt.time()),
        str(end_dt.date()), str(end_dt.time()))
    return filename 

def load_pbufs_to_db(config, mongo, entity_list, start_dt, end_dt, kind = None): 
    """load protocol buffers to mongo"""
    if not kind:
        if len(entity_list) > 0: 
            pb = entity_list[0]
            entity = datastore.Entity._FromPb(entity_pb.EntityProto(pb))
            kind = entity.key().kind()
        else:
            kind = 'unknown'      
    num = 0
    for pb in entity_list: 
        entity = datastore.Entity._FromPb(entity_pb.EntityProto(pb))
        put_document(entity, config, mongo)
        num += 1
        if (num % 100000) == 0:
            g_logger.info(
                "Writing to db for %s from %s to %s. # rows: %d loaded" % (
                kind, start_dt, end_dt, num))
    # assume all entities are from the same kind
    g_logger.info("Writing to db for %s from %s to %s. # rows: %d finishes" % (
        kind, start_dt, end_dt, len(entity_list)))
    kdc.record_progress(mongo, config['coordinator_cfg'],
        kind, start_dt, end_dt, kdc.DownloadStatus.LOADED)

def fetch_and_process_data(kind, start_dt_arg, end_dt_arg, 
    fetch_interval, config): 
    """Main function: fetching data and load it to mongodb.""" 
    mongo = open_db_conn(config)
    kdc.record_progress(mongo, config['coordinator_cfg'],
        kind, start_dt_arg, end_dt_arg, kdc.DownloadStatus.STARTED)
    # fetch
    start_dt = start_dt_arg
    end_dt = end_dt_arg
    g_logger.info("Downloading data for %s from %s to %s starts"  % (
        kind, start_dt_arg, end_dt_arg))
    entity_list = []
    while start_dt < end_dt: 
        next_dt = min(start_dt + dt.timedelta(seconds=fetch_interval), end_dt)
        response = fetch_entities.attempt_fetch_entities(kind, start_dt, 
            next_dt, config['max_logs'], config['max_tries'], False)
        entity_list += pickle.loads(response)
        start_dt = next_dt
    g_logger.info(
        "Data downloaded for %s from %s to %s.# rows: %d finishes" % (
            kind, start_dt_arg, end_dt_arg, len(entity_list)))
    kdc.record_progress(mongo, config['coordinator_cfg'],
        kind, start_dt_arg, end_dt_arg, kdc.DownloadStatus.FETCHED)
    # save to a file
    archived_file = get_archive_file_name(config, kind, 
        start_dt_arg, end_dt_arg) 
    with open(archived_file, 'wb') as f:
        pickle.dump(entity_list, f)
    ret = subprocess.call(["gzip", "-f", archived_file])
    if ret == 0: 
        g_logger.info("%s rows saved to %s.gz" % (len(entity_list), 
            archived_file))
    else: 
        g_logger.error("Cannot gzip %s" % (archived_file))
    kdc.record_progress(mongo, config['coordinator_cfg'],
        kind, start_dt_arg, end_dt_arg, kdc.DownloadStatus.SAVED)
    # load to db
    load_pbufs_to_db(config, mongo, entity_list, 
        start_dt_arg, end_dt_arg, kind)

def apply_transform(doc):
    """transform the document to a format that can be accepted by mongodb """
    if isinstance(doc, dict): 
        for key, value in doc.iteritems():
            doc[key] = apply_transform(value)
        return doc
    elif isinstance(doc, list):  
        doc = [apply_transform(item) for item in doc] 
        return doc
    elif (isinstance(doc, datastore_types.Key) or 
          isinstance(doc, users.User)):
        return str(doc)
    return doc

def open_db_conn(config): 
    """Get a mongodb connection (and reuse it)"""
    def _open_db_conn(config):
        return pymongo.Connection(config['dbhost'], config['dbport'])
    func = db_decorator(config['max_tries'], _open_db_conn) 
    return func(config)

def get_db_name(config, kind): 
    """Return a db connection with kind and config""" 
    if kind not in config['kinds_to_db']:
        return config['default_db']
    return config['kinds_to_db'][kind]    

def ensure_db_indices(config): 
    """Ensure all the indices built""" 
    mongo = open_db_conn(config)
    for kind, indices in COLLECTION_INDICES.iteritems():
         for index in indices:
            ensure_db_index(config, mongo, kind, index)
    
def ensure_db_index(config, mongo, kind, index):
    """ensure index for kind"""
    def _ensure_db_index(config, mongo, kind, index): 
        mongo_db = mongo[get_db_name(config, kind)]
        mongo_db[kind].ensure_index(index)
        
    func = db_decorator(config['max_tries'], _ensure_db_index) 
    func(config, mongo, kind, index)


def put_document(entity, config, mongo): 
    """Put the GAE entity into mongodb"""        
    def _put_document(entity, config, mongo):
        kind = entity.key().kind()
        document = {}
        document.update(entity)
        #make sure all records using the key field as the 
        #index key
        if 'key' not in document:
            document['_id'] = str(entity.key())
        else: 
            document['_id'] = document['key']
        document = apply_transform(document)
        try:
	    mongo_db = mongo[get_db_name(config, kind)]
	    mongo_collection = mongo_db[kind]
            mongo_collection.save(document) 
        except InvalidDocument:
            g_logger.error("InvalidDocument %s" % (document))
        except InvalidStringData as e:
            g_logger.error("Problem inserting doc: %s \n error: %s" % 
               (document, e))
    func = db_decorator(config['max_tries'], _put_document)
    func(entity, config, mongo)
        
def monitor(config, processes):
    """Monitor the concurrent processes"""
    remaining = [] 
    now = time.time()
    for (process, params) in processes:
        if process.is_alive(): 
            if (now - params['start']) > int(config["sub_process_time_out"]):
                #timeout 
                process.terminate()
                #NOTE: Although it get terminated. The duration should be
                # re-scheduled with the upcoming control-db implementation. 
                g_logger.error(
                    "Process hung with kind: %s start_dt: %s end_dt: %s" % (
                    params["kind"], params["start_dt"], params["end_dt"]))      
            else:
                remaining.append((process, params))
    processes = remaining    

def start_data_process(config, start_dt_arg, end_dt_arg) : 
    """Loop through the entity types and perform the main function """
    g_logger.info("Start processing data from %s to %s" %
                  (str(start_dt_arg), str(end_dt_arg)))
    #ensure the db index exist
    ensure_db_indices(config)
    processes = [] 
    for kind, fetch_intervals in config['kinds'].iteritems():
        interval = dt.timedelta(seconds=int(fetch_intervals[0])) 
        fetch_interval = fetch_intervals[1]
        start_dt = start_dt_arg
        end_dt = end_dt_arg
        while start_dt < end_dt:
            if len(active_children()) < config['max_threads']:
                next_dt = min(start_dt + interval, end_dt)
                p = Process(target = fetch_and_process_data,
                    args = (kind, start_dt, next_dt, fetch_interval, config))
                p.start()
                download_params = {"kind": kind, "start_dt": start_dt, 
                                   "end_dt": end_dt, "start": time.time()}
                processes.append((p, download_params))
                start_dt = next_dt
            else: 
                monitor(config, processes)
            #wait for 2 secs to space out the queries
            time.sleep(2)
    while len(active_children()) > 0:
        monitor(config, processes)
        time.sleep(10)

def main():
    options = get_cmd_line_args()
    config = load_unstripped_json(options.config)
    for key in DEFAULT_DOWNLOAD_SETTINGS.keys(): 
        if key not in config: 
            config[key] = DEFAULT_DOWNLOAD_SETTINGS[key]
    if options.start_date and options.end_date: 
        start_dt = fetch_entities.from_date_iso(options.start_date)
        end_dt = fetch_entities.from_date_iso(options.end_date)
    else:
        ts = time.time()
        end_ts = ts - (ts % int(options.proc_interval)) 
        start_ts = end_ts - int(options.proc_interval)
        start_dt = dt.datetime.fromtimestamp(start_ts)
        end_dt = dt.datetime.fromtimestamp(end_ts)
    if options.test:
        config['archive_dir'] = config['test_archive_dir']
    start_data_process(config, start_dt, end_dt)

if __name__ == '__main__':
    main()
