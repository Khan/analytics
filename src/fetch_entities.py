#! /usr/bin/env python
"""Fetching data GAE with protocol buffer calls. It uses
a timestamp field to query for time ranges.  By default,
that property is the "backup_timestamp" property used
by backup_model.BackupModel.
"""

import datetime as dt
import optparse
import pickle
import sys
import time
import urllib
import urllib2

import gae_util
gae_util.fix_sys_path()

from google.appengine.api import datastore
from google.appengine.datastore import entity_pb

import date_util
import oauth_util.fetch_url


# TODO(benkomalo): rename "max_logs" to max_results or something.
def fetch_entities(entity_type, is_ndb, start_date=None, end_date=None,
                   max_logs=None, index_name=None):
    """Makes a request to the main Khan Academy server to download entities.

    Arguments:
        entity_type: The appengine "Kind" for the entity to download.
        is_ndb: Whether or not the entity is an NDB model.
        start_date: A datetime object for the inclusive start time of when
            entities should have been modified to be included in the result.
        end_date: A datetime object for the exclusive end time of when
            entities should have been modified to be included in the result.
        max_logs: The maximum number of entities to return in the result set.
            Note that appengine generally does not handle beyond 10000 (it is
            an open item to fix the server code to handle this with cursors).
        index_name: The entity property to be filtered on for the date range.
    Returns:
        The raw server results from the urlfetch.
    """
    # TODO(benkomalo): move common preprocessing of server results into this
    # method as most clients will probably want to do similar things with
    # errors or deserializing of sorts.

    qs_map = filter(lambda x: x[1], [
        ('is_ndb', int(is_ndb)),
        ('dt_start', date_util.to_date_iso(start_date)),
        ('dt_end', date_util.to_date_iso(end_date)),
        ('max', max_logs),
        ('index', index_name),
    ])
    query_string = urllib.urlencode(qs_map)

    # TODO(david): Send request headers that we accept gzipped data?
    response_url = '/api/v1/dev/protobuf/%s?%s' % (entity_type, query_string)

    return oauth_util.fetch_url.fetch_url(response_url)


def attempt_fetch_entities(kind,
                           is_ndb,
                           start_dt, end_dt,
                           max_logs,
                           max_attempts,
                           index_name,
                           verbose=True):
    """Runs fetch_entities, retrying on errors until max_attempts."""
    if verbose:
        print >> sys.stderr, '[%s] Fetching logs for %s from [%s, %s)...' % (
            dt.datetime.now(), kind, start_dt, end_dt)
    tries = 0
    while True:
        if tries > max_attempts:
            # This is a useful zgrep command when this happens:
            # zgrep -A30 -e "GET /api/v1/dev/protobuf.* HTTP/1.1\" 500"
            raise UserWarning(
                ("Trying %s times to fetch data from GAE but not working. "
                + "You should investigate the server logs.") % tries)
        tries += 1
        sleep_secs = 2 ** tries

        try:
            response = fetch_entities(kind, is_ndb, start_dt, end_dt,
                                      max_logs, index_name)
        except urllib2.HTTPError as e:
            if e.code == 401:
                print >> sys.stderr, "Bad Access Token ...."
            print >> sys.stderr, 'ERROR: %s. Retrying in %s seconds...' % (e,
                 sleep_secs)
            time.sleep(sleep_secs)
            continue
        except urllib2.URLError as e:
            print >> sys.stderr, 'ERROR: %s. Retrying in %s seconds...' % (e,
                sleep_secs)
            time.sleep(sleep_secs)
            continue
        break
    return response


def download_entities(kind,
                      is_ndb,
                      start_dt, end_dt,
                      fetch_interval_seconds,
                      max_entities_per_fetch,
                      max_attempts_per_fetch,
                      index_name,
                      verbose=True):
    """Downloads all entities between start_dt and end_dt  by
    repeatedly calling attempt_fetch_entities if necessary.  Multiple calls
    are only necessary if there are more entities in the time interval
    than max_entities_per_fecth.

    WARNING: because the API call returns entities in [start_dt, end_dt),
    this, function may return some duplicates in its result.  The caller should
    de-dupe by .key() of the entities if needed.

    Returns a list of Entities in protocol buffer format.
    """

    entity_list = []
    interval_start = start_dt
    time_delta = dt.timedelta(seconds=fetch_interval_seconds)
    while interval_start < end_dt:
        interval_end = min(interval_start + time_delta, end_dt)
        response = attempt_fetch_entities(kind,
                                          is_ndb,
                                          interval_start, interval_end,
                                          max_entities_per_fetch,
                                          max_attempts_per_fetch,
                                          index_name,
                                          verbose)
        response_list = pickle.loads(response)
        entity_list += response_list

        if len(response_list) == max_entities_per_fetch:
            # if we maxed out the number of entities for the fetch, there
            # might still be more so query again from the last timestamp
            # WARNING: this depends on the implementation of the API call
            # returning the protobuffs in sorted order
            #
            # This works for both db and ndb models. To convert protobufs to
            # ndb models you'd need to import the model and use a ndb
            # ModelAdapter. But here we just need access to the
            # backup_timestamp property (index_name), so deserializing the
            # protobuf into the lower-level Entity will suffice.
            pb = response_list[-1]
            entity = datastore.Entity._FromPb(entity_pb.EntityProto(pb))
            if index_name in entity:
                interval_end = entity[index_name]
        interval_start = interval_end
    return entity_list


def get_cmd_line_args():
    today_dt = dt.datetime.combine(dt.date.today(), dt.time())
    yesterday_dt = today_dt - dt.timedelta(days=1)

    parser = optparse.OptionParser(usage="%prog [options]",
        description="Fetches problem logs from khanacademy.org using its "
                    "v1 API. Outputs in pickled entities.")
    parser.add_option("-s", "--start_date",
        default=date_util.to_date_iso(yesterday_dt),
        help="Earliest inclusive date of logs to fetch, in ISO 8601 format. "
             "Defaults to yesterday at 00:00.")
    parser.add_option("-e", "--end_date",
        default=date_util.to_date_iso(today_dt),
        help="Latest exclusive date of logs to fetch, in ISO 8601 format. "
             "Defaults to today at 00:00.")
    parser.add_option("-i", "--interval", default=10,
        help="Time interval to fetch at a time, in seconds. Defaults to 10.")
    parser.add_option("-l", "--max_logs", default=1000,
        help="Max # of log entries to fetch per interval. Defaults to 1000.")
    parser.add_option("-r", "--max_retries", default=8,
        help="Maximum # of retries for request attempts before failing. "
             "Defaults to 8.")
    parser.add_option("-o", "--output_file",
        help="Name of the file to output.")
    parser.add_option("-t", "--type",
        help="Entity type to back up")
    parser.add_option("-n", "--ndb", help="Entity is an NDB model",
        action="store_true", dest="is_ndb", default=False)
    parser.add_option("-k", "--key", default="backup_timestamp",
        help="Name of entity property to use in a date range query.")

    options, _ = parser.parse_args()

    if not options.type:
        print >> sys.stderr, 'Please specify an entity type to back up'
        exit(1)
    if not options.output_file:
        options.output_file = options.type + ".pickle"

    return options


def main():
    options = get_cmd_line_args()
    end_dt = date_util.from_date_iso(options.end_date)
    start_dt = date_util.from_date_iso(options.start_date)

    entity_list = download_entities(options.type,
                                    options.is_ndb,
                                    start_dt, end_dt,
                                    int(options.interval),
                                    int(options.max_logs),
                                    int(options.max_retries),
                                    options.key)

    with open(options.output_file, 'wb') as f:
        pickle.dump(entity_list, f)

    print >> sys.stderr, ("Downloaded and wrote %d entities.  Exiting." %
                          len(entity_list))


if __name__ == '__main__':
    main()
