#!/usr/bin/env python
"""One time script to back populate ProblemLog and VideoLog pickle files"""

import datetime as dt
import time
import gzip
import pickle
import re

from optparse import OptionParser
from multiprocessing import Process, active_children

import gae_download
import util


g_logger = util.get_logger()

def get_cmd_line_args():
    parser = OptionParser(usage="%prog [options]",
        description="Back populate data to mongo")
    parser.add_option("-c", "--config", 
        help="json config same as gae_download ")
    parser.add_option("-f", "--file_list", 
        help="file that contain list of files")
    
    options, extra_args = parser.parse_args()

    if not options.config:
        g_logger.fatal('Please specify the json config file')
        exit(1)
    if not options.file_list:
        g_logger.fatal(
            "Please specify the file that contains list of pickle files") 
        exit(1)
    return options
def gz_pickle_to_mongo(config, gzfile):
    mongo = gae_download.open_db_conn(config)
    (year, month, day)  = re.match(r'.*(\d{4})-(\d{2})-(\d{2})', gzfile).groups()
    start_dt = dt.datetime(int(year), int(month), int(day))
    end_dt = start_dt + dt.timedelta(days=1)
    f = gzip.open(gzfile, "rb") 
    entity_list = pickle.load(f)
    f.close()
    g_logger.info("Loading %s entries to db" % len(entity_list))
    gae_download.load_pbufs_to_db(config, mongo, entity_list, start_dt, end_dt) 
def monitor(config, processes): 
    """Monitor the concurrent processes"""
    remaining = [] 
    now = time.time()
    for (process, gzfile, ts) in processes:
        if process.is_alive(): 
            if (now - ts) > int(config["sub_process_time_out"]):
                #timeout 
                process.terminate()
                g_logger.error( "Process hung with file: %s " % gzfile)
            else:
                remaining.append((process, gzfile, ts))
    processes = remaining
def main():
    options = get_cmd_line_args()
    config = util.load_unstripped_json(options.config)
    #hard code some args
    config['max_threads'] = 2
    config['coordinator_cfg']['control_db'] = "ka_backpopulate_cntrl"
    config["sub_process_time_out"] = 86400*3
    with open(options.file_list) as f:
        file_list = f.readlines()
    processes = []
    for gzfile in file_list:
        while True:
            if len(active_children()) < config['max_threads']:         
               g_logger.info("Starting loading %s ...", gzfile)
               p = Process(target = gz_pickle_to_mongo,
                           args = (config, gzfile.strip()))
               processes.append((p, gzfile.strip(), time.time()))
               p.start()
               time.sleep(5)
               break
            else: 
               monitor(config, processes)
               time.sleep(10)
    while len(active_children()) > 0:       
        monitor(config, processes)
        time.sleep(10)
if __name__ == '__main__':
    main()
    
