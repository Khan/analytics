#!/usr/bin/env python
"""Script to download data from the Google App Engine Data Store.  It takes a
json config file to specify what entity types to download and the detailed
download configurations.  The program also takes the start_date and end_date
 to specify the data duration we would like to download.  All the entity types
 have to have the field "backup_timestamp" to be downloaded properly.
See the config under ../../cfg for more details

TODO(yunfang): adding a controldb in mongo to coordinate the fetch and
               db load of gae data
"""

import datetime as dt
import json
import os
import pickle
import re
import subprocess
import sys
import time

from optparse import OptionParser
from multiprocessing import active_children
from multiprocessing import Process

import pymongo

import gae_util
gae_util.fix_sys_path()

sys.path.append(os.path.dirname(__file__) + "/../map_reduce/py")
import load_pbufs_to_hive

import date_util
import fetch_entities
from util import (mkdir_p, load_unstripped_json,
                  get_logger, db_decorator)
import ka_download_coordinator as kdc
import notify

DEFAULT_DOWNLOAD_SETTINGS = {
    "max_threads": 4,  # max number of parrellel threads
    "max_tries": 8,  # max number of tries to download entities
    "interval": 120,  # data accumulated before writing into mongodb
    "sub_process_time_out": 10800,  # sub process timeout in seconds (3 hours)
    "max_logs": 1000,  # max number of entities from gae foreach pbuf call
    "dbhost": "localhost",
    "dbport": 28017,
    "default_db": "testdb",  # dbname to write to
    "archive_dir": "archive"
}

g_logger = get_logger()


def get_cmd_line_args():
    parser = OptionParser(usage="%prog [options]",
        description="Download data from the Google App Engine Datastore")
    parser.add_option("-c", "--config",
        help="JSON config file for all the download details")

    # NOTE: start_date and end_date defaults are setup in main(), which is
    # generally the start of the last proc_interval, which is usually 1 hour
    parser.add_option("-s", "--start_date",
        help="Earliest inclusive date of logs to fetch, in ISO 8601 format. "
              "Defaults to yesterday at 00:00.")
    parser.add_option("-e", "--end_date",
        help="Latest exclusive date of logs to fetch, in ISO 8601 format. "
              "Defaults to today at 00:00.")
    parser.add_option("-d", "--archive_dir",
        help="The directory to archive the downloaded protobufs. Will "
             "override the value in the JSON config if specified.")
    parser.add_option("-p", "--proc_interval", default=3600,
        help="process interval if no start_date end_date specified")

    options, _ = parser.parse_args()
    if not options.config:
        g_logger.fatal('Please specify the json config file')
        exit(1)
    return options


def get_archive_file_name(config, kind, start_dt, end_dt, ftype='pickle'):
    """Get the archive file name. has the format of
    {ARCHIVE_DIR}/YY-mm-dd/{kind}/kind-start_dt-end_dt.pickle

    """

    # Note that Hadoop does not like leading underscores in files, so we strip
    # out leading underscores (as may be used in the case of private classes)
    kind = re.sub(r'^_*', '', kind)

    datestr = str(start_dt.date())
    dirname = "%s/%s/%s" % (config['archive_dir'], datestr, kind)
    mkdir_p(dirname)
    filename = "%s/%s-%s-%s-%s-%s.%s" % (dirname, kind,
        str(start_dt.date()), str(start_dt.time()),
        str(end_dt.date()), str(end_dt.time()), ftype)
    return filename


def log_timestamp_outside_window(kind, raw_timestamp, start_dt, end_dt):
    """Gets the backup_timestamp, and logs a message if its outside the window.
    """

    try:
        actual_timestamp = float(raw_timestamp)

        if actual_timestamp > 0:
            actual_timestamp = dt.datetime.fromtimestamp(actual_timestamp)
        else:
            raise ValueError

        if actual_timestamp < start_dt:
            # future - past -> postive timedelta
            diff = start_dt - actual_timestamp

            g_logger.info(("backup_timestamp on a %s = %s was actually "
                "before the window %s to %s by %s") %
                (kind, actual_timestamp, start_dt, end_dt, diff))
        elif actual_timestamp > end_dt:
            # past - future -> negative timedelta
            diff = end_dt - actual_timestamp

            g_logger.info(("backup_timestamp on a %s = %s was actually "
                "after the window %s to %s by %s") %
                (kind, actual_timestamp, start_dt, end_dt, diff))

    except ValueError:
        # We ignore badly formatted data, since this is debug only
        pass


def fetch_and_process_data(kind, start_dt_arg, end_dt_arg,
    fetch_interval, config):
    """Main function: fetching data and load it to mongodb."""
    if config['dbhost']:
        mongo = open_db_conn(config)
        kdc.record_progress(mongo, config['coordinator_cfg'],
            kind, start_dt_arg, end_dt_arg, kdc.DownloadStatus.STARTED)

    # fetch
    g_logger.info("Downloading data for %s from %s to %s starts" % (
        kind, start_dt_arg, end_dt_arg))
    is_ndb = bool(config['kinds'][kind][3])
    entity_list = fetch_entities.download_entities(
                      kind,
                      is_ndb,
                      start_dt_arg, end_dt_arg,
                      fetch_interval,
                      config['max_logs'], config['max_tries'],
                      "backup_timestamp",  # TODO(jace): make configurable
                      verbose=False)
    g_logger.info(
        "Data downloaded for %s from %s to %s.# rows: %d finishes" % (
            kind, start_dt_arg, end_dt_arg, len(entity_list)))

    if config['dbhost']:
        kdc.record_progress(mongo, config['coordinator_cfg'],
            kind, start_dt_arg, end_dt_arg, kdc.DownloadStatus.FETCHED)

    # save to a file
    # TODO(yunfang): revisit if we should save the pickled pb
    archived_file = get_archive_file_name(config, kind,
        start_dt_arg, end_dt_arg, 'pickle')
    with open(archived_file, 'wb') as f:
        pickle.dump(entity_list, f)
    ret = subprocess.call(["gzip", "-f", archived_file])
    if ret == 0:
        g_logger.info("%s rows saved to %s.gz" % (len(entity_list),
            archived_file))
    else:
        g_logger.error("Cannot gzip %s" % (archived_file))

    #jsonize the entities
    json_filename = get_archive_file_name(config, kind,
        start_dt_arg, end_dt_arg, 'json')
    json_key = config['kinds'][kind][4]
    f = open(json_filename, 'wb')
    for pb in entity_list:
        doc = load_pbufs_to_hive.pb_to_dict(pb)

        # TODO(mattfaus): Make configurable, like for download_entities() above
        log_timestamp_outside_window(
            kind, doc.get('backup_timestamp'), start_dt_arg, end_dt_arg)

        json_str = json.dumps(doc)
        print >>f, "%s\t%s" % (doc[json_key], json_str)
    f.close()
    ret = subprocess.call(["gzip", "-f", json_filename])
    if ret == 0:
        g_logger.info("%s rows saved to %s.gz" % (len(entity_list),
            json_filename))
    else:
        g_logger.error("Cannot gzip %s" % (json_filename))

    if config['dbhost']:
        kdc.record_progress(mongo, config['coordinator_cfg'],
            kind, start_dt_arg, end_dt_arg, kdc.DownloadStatus.SAVED)

        # Well, we didn't actually load the data with this script, but mark
        # it as such anyway.
        kdc.record_progress(mongo, config['coordinator_cfg'],
            kind, start_dt_arg, end_dt_arg, kdc.DownloadStatus.LOADED)


def open_db_conn(config):
    """Get a mongodb connection (and reuse it)"""
    def _open_db_conn(config):
        return pymongo.Connection(config['dbhost'], config['dbport'])
    func = db_decorator(config['max_tries'], _open_db_conn)
    return func(config)


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
                msg = (("Process hung with kind: %s" + 
                        " start_dt: %s end_dt: %s" +
                        " after %s seconds") % (
                        params["kind"], params["start_dt"], 
                        params["end_dt"], config["sub_process_time_out"]))
                g_logger.error(msg)
                notify.send_hipchat(msg)
                notify.send_email("WARNING: gae subprocess hung", msg)
            else:
                remaining.append((process, params))
    processes = remaining


def start_data_process(config, start_dt_arg, end_dt_arg):
    """Loop through the entity types and perform the main function """
    g_logger.info("Start processing data from %s to %s" %
                  (str(start_dt_arg), str(end_dt_arg)))

    processes = []
    for kind, fetch_intervals in config['kinds'].iteritems():
        # fetch_intervals is an array with format [int, int, bool, bool, str]
        # [Save interval, fetch interval, isMutable, is_ndb, json key]
        interval = dt.timedelta(seconds=int(fetch_intervals[0]))
        fetch_interval = fetch_intervals[1]
        start_dt = start_dt_arg
        end_dt = end_dt_arg
        while start_dt < end_dt:
            if len(active_children()) < config['max_threads']:
                next_dt = min(start_dt + interval, end_dt)
                p = Process(target=fetch_and_process_data,
                    args=(kind, start_dt, next_dt, fetch_interval, config))
                p.start()
                download_params = {"kind": kind, "start_dt": start_dt,
                                   "end_dt": next_dt, "start": time.time()}
                processes.append((p, download_params))
                start_dt = next_dt
            else:
                monitor(config, processes)
            # wait for 2 secs to space out the queries
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
        start_dt = date_util.from_date_iso(options.start_date)
        end_dt = date_util.from_date_iso(options.end_date)
    else:
        ts = time.time()
        end_ts = ts - (ts % int(options.proc_interval))
        start_ts = end_ts - int(options.proc_interval)
        start_dt = dt.datetime.fromtimestamp(start_ts)
        end_dt = dt.datetime.fromtimestamp(end_ts)
    if options.archive_dir:
        # Override the archive directory, if specified.
        config['archive_dir'] = options.archive_dir
    start_data_process(config, start_dt, end_dt)


if __name__ == '__main__':
    main()
    # Write token file upon completion. gae_download.py runs under an hourly,
    # cronjob, so load_emr_daily.sh will start only when all 24 token files
    # are present.
    dirname = "/home/analytics/kabackup/daily_new/tokens/"
    filename = "token%s.txt" % dt.datetime.now().hour
    f = open(dirname + filename, "w"):
    f.close()
