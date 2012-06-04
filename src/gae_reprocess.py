#!/usr/bin/env python
"""Scan the monitor database and reprocess any failed GAE tasks"""

import datetime
import optparse
import re

import pymongo

import date_util
import gae_download
import ka_download_coordinator
import util


g_logger = util.get_logger()


def get_cmd_line_args():
    parser = optparse.OptionParser(usage="%prog [options]",
        description="reprocess failed logs")
    # Same conf as gae_download
    parser.add_option("-c", "--config", help="same conf as GAE download")
    # Only process tasks started after this date
    parser.add_option("-s", "--start_date",
        default="1970-01-01T00:00:00Z",
        help="Only tasks started after this date or later")
    options, _ = parser.parse_args()

    if not options.config:
        g_logger.fatal("Please specify the conf file")
        exit(1)
    return options


def main():
    options = get_cmd_line_args()
    config = util.load_unstripped_json(options.config)
    g_logger.info("Fetching failed jobs from progress db")
    start_dt = date_util.from_date_iso(options.start_date)
    mongo = gae_download.open_db_conn(config)
    coordinator_cfg = config["coordinator_cfg"]
    # Don't touch tasks that was recently started
    # TODO(yunfang): parameterize this
    two_hours_ago = datetime.datetime.now() - datetime.timedelta(hours=2)
    results = ka_download_coordinator.get_failed_jobs(mongo, coordinator_cfg)
    if not results:
        g_logger.info("Empty result set. Nothing to reprocess.")
        exit(0)
    for rec in results:
        if rec["history"]["1"] < start_dt:
            continue
        if rec["history"]["1"] >= two_hours_ago:
            # Started less than 2 hours ago
            continue
        # Reprocess
        fetch_interval = config['kinds'][rec['kind']][1]
        gae_download.fetch_and_process_data(rec["kind"], rec["start_dt"],
            rec["end_dt"], fetch_interval, config)
    g_logger.info("Done reprocessing!!")


if __name__ == '__main__':
    main()
