#!/usr/bin/env python
""" This script greps the downloaded logs for info related to a google issue.

Basic use is ./greplog.py -i [issue #]. It will grab the time the issue was
reported (change it with -t <timestamp> argument and look for log records by
default 20 minutes before (change it with -d <seconds> argument). It will
filter the logs by the bingo_id if it was added to the issue (change it with
-b <bingo_id> option) and if not it will filter by user_agent string (change it
with -u option).  For a full list of options do issue_finder.py --help.  You
can also grep for only log lines that return errors.

All log lines are printed out and you can redirect them to a temporary file for
analysis such as greplog.py -i 19618 > issue_19618.log.
"""

import datetime
import gzip
import json
import optparse
import os
import re
import sys
import urllib

sys.path.insert(0, "../map_reduce/py")

import raw_log_to_request_log_mapper as rlm


BASE_DIR = "/home/analytics/kalogs/"
ISSUES_URL = "http://code.google.com/p/khanacademy/issues/csv?can=1&q=id%%3D%i&colspec=Opened+Summary&sort=-ID"
DEFAULT_TIME_DELTA = 1200  # 20 minutes
USAGE = """[options]

Search appengine logs for requests related to a particular issue.
"""


def get_issue_details(issue_id):
    # Caching the external call as some people might want to search the same
    # issue a few times with different end timestamp or time deltas.
    issue_detail_cache_file = "/tmp/issue_detail_%i" % issue_id
    try:
        f = open(issue_detail_cache_file)
        data = f.read()
        return json.loads(data)
    except IOError:
        pass

    issue_file_handler = urllib.urlopen(ISSUES_URL % issue_id)
    issue_csv = issue_file_handler.read()

    # Parse the CSV file
    issues = issue_csv.rstrip().split("\n")
    issue = issues[1]  # Headers are on line 0, ignore it
    # issue line will look like:
    # "May 14, 2013 14:33:56","1368542036","can't enter students in my class","BingoId-_gae_bingo_randomW5fY6o9t4naJ9yuI9wjqWjb4YdBEBleJtplf0Rup, Referer-httpwww.khanacademy.orgstudents, Type-Defect, UserAgent-Mozilla5.0compatibleMSIE9.0WindowsNT6.1WOW64Trident5.0"  @Nolint
    fields = issue.split(",")  # Fields are separated by , but labels are also
    # First two fields are the opened date in two pieces because of the ,
    timestamp = int(fields[2].replace('"', ''))  # Third field is the timestamp
    labels = fields[4:]  # Ignore summary in fouth field, rest are labels
    # First label will being with quote others with space, last label ends with
    # a quote
    labels = [label.strip(' "') for label in labels]

    bingo_id = None
    referer = None
    user_agent = None
    for label in labels:
        if label.startswith("BingoId-"):
            bingo_id = label[8:]
            # The bingo_id is stored url quoted in our logs.  If it is not
            # already quoted then we quote it here to be able to filter it
            # correctly.
            if (bingo_id.startswith("_gae_bingo_random") and
                bingo_id[17:19] != "%3A"):
                bingo_id = bingo_id[0:17] + "%3A" + bingo_id[17:]
        elif label.startswith("Referer-"):
            referer = label[8:]
        elif label.startswith("UserAgent-"):
            user_agent = label[10:]

    ret_val = (timestamp, bingo_id, referer, user_agent)

    # Write to temp cache file
    f = open(issue_detail_cache_file, "w")
    f.write(json.dumps(ret_val))
    return ret_val


def parse_log_file(input_file_name,
                   target_timestamp,
                   timedelta=DEFAULT_TIME_DELTA,
                   errors_only=False,
                   target_bingo_id=None,
                   target_url=None,
                   target_user_agent=None):
    """ Searches the log file near the timestamp for a users entries

        Iterates over the log and finds all entries within timedelta of the
        target_timestamp and then prints them to stdout.
    """

    if not(errors_only
           or target_bingo_id
           or target_url
           or target_user_agent):
        raise Exception("Search too broad, need at least one of errors_only, "
                        "target_bingo_id, target_url, or target_user_agent")

    input_file = gzip.open(input_file_name)

    for (request_log_line, request_log_match, app_log_lines) in (
         rlm.RequestLogIterator(input_file)):

        if request_log_match is None:
            # RequestLogIterator encountered the 'sentinel'
            continue

        if errors_only:
            status = request_log_match.group("status")
            if int(status) < 400:
                continue

        datetimestamp_string = request_log_match.group("time_stamp")
        datetimestamp = datetime.datetime.strptime(datetimestamp_string[:-6],
                                                   "%d/%b/%Y:%H:%M:%S")
        offset = int(datetimestamp_string[-5:])
        tz_delta = datetime.timedelta(hours=offset / 100)
        timestamp = int(datetime.datetime.strftime(
            datetimestamp - tz_delta, "%s"))

        # TODO(james): binary search the log file to get to the target start
        # time quickly
        if (timestamp < target_timestamp - timedelta):
            continue

        # TODO(james): sometimes the log file seems out of order by a couple of
        # seconds.  Determine if we want a buffer here.
        if timestamp > target_timestamp:
            return

        if target_bingo_id:
            bingo_id = None
            # Get the bingo_id if it exists in the app logs
            for line in app_log_lines:
                kalog_match = rlm._KA_LOG_MATCHER.match(line)
                if kalog_match:
                    bingo_id = kalog_match.group("bingo_id")
                    break

            if target_bingo_id != bingo_id:
                continue

        if target_url:
            url = request_log_match.group("url")
            # We want to match the referer as well as the bug might have been
            # in an ajax request that was sent from that url.
            referer = request_log_match.group("referer")
            # The target url if coming from a google issue might be stipped of
            # all slashes
            # TODO(james): make sure this is the same stripping that happens in
            # google issue labels
            stripped_url = re.sub("[^\w.]", "", url)
            stripped_referer = re.sub("[^\w.]", "", referer)
            if (target_url != url and
                target_url != referer and
                target_url != stripped_url and
                target_url != stripped_referer):
                continue

        if target_user_agent:
            # When we don't have a bingo_id, the user_agent might be a way to
            # pseudo identify the bug.
            user_agent = request_log_match.group("user_agent")
            # TODO(james): make sure this is the same stripping that happens in
            # google issue labels
            stripped_user_agent = re.sub("[^\w.]", "", user_agent)
            if (target_user_agent != user_agent and
                target_user_agent != stripped_user_agent):
                continue

        print request_log_line
        for line in app_log_lines:
            # Tracebacks can have the tabs and newlines escaped. We unescape
            # them here for readability
            print line.replace('\\t', '\t').replace('\\n', '\n')
        print ''


def find_log_files_for(timestamp, time_delta=DEFAULT_TIME_DELTA):
    """Find the files with logs lines in time_delta seconds before timestamp

    Files on the analytics machine are stored in UTC.
    """
    start_timestamp = timestamp - time_delta
    start_hour_timestamp = start_timestamp - start_timestamp % 3600
    start_hour_datetime = datetime.datetime.fromtimestamp(start_hour_timestamp)
    end_datetime = datetime.datetime.fromtimestamp(timestamp)
    files = []
    current_hour_datetime = start_hour_datetime
    while current_hour_datetime < end_datetime:
        filename = (BASE_DIR +
                    datetime.datetime.strftime(current_hour_datetime,
                                               "%Y/%m/%d/%H:00:00Z.log.gz"))
        files.append(filename)
        current_hour_datetime += datetime.timedelta(hours=1)
    return files


if __name__ == '__main__':
    parser = optparse.OptionParser(USAGE)
    parser.add_option('--issue-id', '-i', dest='issue_id', type=int,
                      help=('A google issue id on which to filter the logs '
                            'by. The script will search the minutes preceding '
                            'when the issue was filed and will use either the '
                            'bingo_id if it exists in the issue labels or the '
                            'user agent to further filter the logs by.'))
    parser.add_option('--bingo-id', '-b', dest='bingo_id', type=str,
                      help=('Filter the logs for this bingo-id. It will '
                            'override any bingo_id retrieved from the issue'))
    parser.add_option('--errors-only', '-e', dest='errors_only',
                      action='store_true',
                      help='Only output logs that are errors.')
    parser.add_option('--timestamp', '-t', dest='timestamp', type=int,
                      help=('Timestamp for the end of the time range to look '
                            'at.'))
    parser.add_option('--timedelta', '-d', dest='time_delta', type=int,
                      default=DEFAULT_TIME_DELTA,
                      help=('How long before the timestamp you want to search '
                            '(in seconds).'))
    parser.add_option('--use-url', '-u', dest='use_url', action='store_true',
                      help=('Whether to filter the logs by the url/referer '
                            'url found in the referrer referenced in the '
                            'issue.'))
    parser.add_option('--url', '-l', type=str, dest='url',
                      help='Url to filter by.')
    parser.add_option('--user-agent', '-a', dest='user_agent', type=str,
                      help=('Filter the log using this user-agent string. '
                            'This will only be used if the bingo_id is not '
                            'set either in the issue or manually with -b'))

    options, args = parser.parse_args()

    target_url = None
    target_timestamp = None
    target_bingo_id = None
    target_url = None
    condition_string = ""
    if options.issue_id:
        (target_timestamp, target_bingo_id, referer, target_user_agent) = (
            get_issue_details(options.issue_id))
        if options.use_url and referer:
            target_url = referer

    # Overrides for what the issue lists
    if options.timestamp:
        target_timestamp = options.timestamp
    if options.bingo_id:
        target_bingo_id = options.bingo_id
    if options.user_agent:
        target_user_agent = options.user_agent
    if options.url:
        target_url = options.url

    if target_bingo_id:
        # Don't bother also filtering for user_agent if we have a bingo_id
        target_user_agent = None

    time_delta = options.time_delta
    file_names = find_log_files_for(int(target_timestamp), time_delta)

    if target_bingo_id:
        condition_string = "bingo_id = %s" % target_bingo_id
    elif target_user_agent:
        condition_string = "user_agent = %s" % target_user_agent

    if target_url:
        if condition_string:
            condition_string += " and the url or referer = %s" % target_url
        else:
            condition_string = "url or referer = %s" % target_url

    target_datetime = datetime.datetime.fromtimestamp(float(
        target_timestamp))

    print "Searching %s for %s in the %i minutes before %s where the %s" % (
        file_names,
        "all errors" if options.errors_only else "all requests",
        int(time_delta / 60),
        datetime.datetime.strftime(target_datetime, "%d/%b/%Y:%H:%M:%S"),
        condition_string)

    for file_name in file_names:
        parse_log_file(file_name,
                       target_timestamp,
                       time_delta,
                       options.errors_only,
                       target_bingo_id,
                       target_url,
                       target_user_agent)

