#!/usr/bin/env python

"""Script to download GAE/Bingo experiment data from GAE.

This is separate from gae_download.py as the experiment info downloaded is
somewhat massaged and not quite in a raw entity format, like the other data.
"""

import datetime
import json
from optparse import OptionParser
import os
import subprocess
import sys
import time
import urllib2

import oauth_util.fetch_url
import util


logger = util.get_logger()


def get_cmd_line_args():
    parser = OptionParser(
            usage="%prog [options]",
            description="Downloads GAE/Bingo experiment data from GAE")
    parser.add_option(
            "-d", "--archive_dir",
            default="archive",
            help="The directory to archive the downloaded experiments.")
    parser.add_option(
            "-s", "--date_subdiretory",
            help="The date subdirectory under archive_dir to store "
                 "the data in.  Format:  YYYY-MM-DD.  Note that download "
                 "API will always return the most recent data.  This doesn't "
                 "change what is downloaded, only where it is written.")

    options, _ = parser.parse_args()
    return options


def fetch_experiments(archived=False):
    max_attempts = 3
    tries = 0
    param = "archives=1" if archived else "archives=0"
    while tries < max_attempts:
        tries += 1
        sleep_secs = 2 ** tries

        try:
            response_url = '/api/v1/dev/bingo_experiments?%s' % param
            logger.info("Attempting fetch to [%s]" % response_url)
            return json.loads(oauth_util.fetch_url.fetch_url(response_url))
        except urllib2.HTTPError as e:
            if e.code == 401:
                logger.error("Bad Access Token")
            logger.error("ERROR: %s. Retrying in %ss..." % (e, sleep_secs))
            time.sleep(sleep_secs)
            continue
        except urllib2.URLError as e:
            logger.error("ERROR: %s. Retrying in %ss..." % (e, sleep_secs))
            time.sleep(sleep_secs)
            continue

    raise Exception("Unable to fetch data.")


def dump_alternatives(experiment, fout=sys.stdout):
    """Emits experiment alternative rows given a single experiment
    JSON from the server."""

    hashable_name = experiment['family_name'] or experiment['canonical_name']
    canonical_name = experiment['canonical_name']
    dt_started = experiment['dt_started']
    live = experiment['live']

    for alternative in experiment['alternatives']:
        fout.write("\t".join([
                canonical_name,
                str(alternative['content']),
                hashable_name,
                str(alternative['weight']),
                dt_started,
                str(live),
                str(alternative['number'])
            ]) + "\n")
    fout.flush()


def fetch_and_process_data(options):
    """Fetches experiment data and dumps content to local files."""

    logger.info("Downloading GAEExperiment data")

    experiments = (fetch_experiments(archived=False) +
                   fetch_experiments(archived=True))
    logger.info("Downloaded %s experiments" % len(experiments))

    date_subdir = str(datetime.date.today())
    if options.date_subdiretory:
        date_subdir = options.date_subdiretory

    # Named ".json" to be consistent with the other data files that have
    # JSON in them. They're just raw tab-delimited text files, though.
    out_path = "%s/%s/bingo_alternative_info/bingo_alternative_info.json" % (
            options.archive_dir, date_subdir)

    if not os.path.exists(os.path.dirname(out_path)):
        os.makedirs(os.path.dirname(out_path))

    with open(out_path, 'wb+') as f:
        for experiment in experiments:
            dump_alternatives(experiment, f)

    ret = subprocess.call(["gzip", "-f", out_path])
    if ret == 0:
        logger.info(
                "%s experiments saved to %s.gz" % (len(experiments),
                out_path))
    else:
        logger.error("Cannot gzip %s" % (out_path))


def main():
    options = get_cmd_line_args()
    fetch_and_process_data(options)


if __name__ == '__main__':
    main()

