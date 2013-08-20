#!/usr/bin/env python
""" This script greps the downloaded logs and prints results with context

This is particularly useful for debugging google issues.
Basic use is greplog -i [issue #]. It will grab the time the issue was
reported (change it with -t <timestamp> argument) and look for log records by
default 20 minutes before (change it with -d <minutes> argument). It will
filter the logs by the bingo_id if it was added to the issue (change it with
-b <bingo_id> option) and if not it will filter by user_agent string (change it
with -u option).  You can also filter by the request status code with -s.
For a full list of options do issue_finder.py --help.

All log lines are printed to stdout and you can redirect them to a temporary
file for analysis such as: greplog -i 19618 > /tmp/issue_19618.log
"""

import contextlib
import csv
import datetime
import json
import optparse
import os
import re
import subprocess
import sys
import time
import urllib

sys.path.insert(0, os.path.join(os.path.dirname(__file__),
                                '..', 'map_reduce', 'py'))

import raw_log_to_request_log_mapper as rlm


BASE_DIR = "/home/analytics/kalogs/"
# can=1 refers to all issues (open, closed, etc...)
# q=id%%3D%i should filter issues to where its id = %i
# colspec=Opened+Summary specifies which columns to return
# Opened includes both the date as a string and a timestamp field
# Summary includes both the issue title and a concatentation of all labels
ISSUES_URL = "http://code.google.com/p/khanacademy/issues/csv?can=1&q=id%%3D%i&colspec=Opened+Summary"
DEFAULT_TIME_DELTA = 20  # minutes
USAGE = """[options] ('<search query string>')

Grep the request logs and return the results together with their context (ie.
If you search on a phrase that appears in an error message it will return the
full traceback and the request line it was associated with.)

Instead of like normal grep when you must pass the file in, greplog will find
the correct files based upon the issue id or a timestamp in the options, or if
none are provided then will searches near the end of the most recent fully
downloaded file.

If an issue_id or other filtering options are used, then it is not necessary
to provide a search query string.

Examples:
    greplog -i 19618 # Look in the 20 minutes before the issue 19618
                     # was reported
    greplog -i 19618 -d 120 # Same search but look in the preceeding 2 hours
                            # instead
    greplog 'out of order' -d 60 # Search the latest file downloaded for all
                                 # requests with 'out of order' in its app logs
    greplog -s 500 -d 60 # Search the latest file downloaded for any error
"""


def get_issue_details(issue_id):
    """ Get the timestamp, bingo_id, referer, and user_agent from issue.

        We make a call to get a csv file of a list which should only contain
        the one issue with the id specified. The timestamp is its own field,
        while the bingo_id, referer, and user_agent may or may not be in the
        issue labels (which come with the Summary) depending upon whether they
        came from our site directly to report the issue.
    """

    # Caching the external call as some people might want to search the same
    # issue a few times with different end timestamp or time deltas.
    issue_detail_cache_file = "/tmp/issue_detail_%i" % issue_id
    if os.path.isfile(issue_detail_cache_file):
        with open(issue_detail_cache_file) as f:
            data = f.read()
            return json.loads(data)

    url = ISSUES_URL % issue_id
    with contextlib.closing(urllib.urlopen(url)) as issue_file_handler:
        issue_reader = csv.reader(issue_file_handler)
        # Ignore the headers
        issue_reader.next()

        # issue line will look like:
        # "May 14, 2013 14:33:56","1368542036","can't enter students in my class","BingoId-_gae_bingo_randomW5fY6o9t4naJ9yuI9wjqWjb4YdBEBleJtplf0Rup, Referer-httpwww.khanacademy.orgstudents, Type-Defect, UserAgent-Mozilla5.0compatibleMSIE9.0WindowsNT6.1WOW64Trident5.0"  @Nolint
        (date_string, timestamp, title, labels_string) = issue_reader.next()
        timestamp = int(timestamp)
        labels = labels_string.split(",")

        bingo_id = None
        referer = None
        user_agent = None
        for label in labels:
            if label.startswith("BingoId-"):
                bingo_id = label[len("BingoId-"):]
                # The bingo_id is stored url quoted in our logs. Google issues
                # will have stripped out the : from the bingo_id so we need to
                # put it back in here.
                if (bingo_id.startswith("_gae_bingo_random") and
                    bingo_id[len("_gae_bingo_random"):
                             len("_gae_bingo_random") + 2] != "%3A"):
                    bingo_id = (bingo_id[0:len("_gae_bingo_random")] + "%3A" +
                                bingo_id[len("_gae_bingo_random"):])
            elif label.startswith("Referer-"):
                referer = label[len("Referer-"):]
            elif label.startswith("UserAgent-"):
                user_agent = label[len("UserAgent-"):]

        ret_val = (timestamp, bingo_id, referer, user_agent)

        # Write to temp cache file
        with open(issue_detail_cache_file, "w") as f:
            json.dump(ret_val, f)
        return ret_val


def parse_log_file(input_file_name,
                   target_timestamp,
                   search_string=None,
                   timedelta=DEFAULT_TIME_DELTA,
                   target_status=None,
                   target_bingo_id=None,
                   target_url=None,
                   target_user_agent=None):
    """ Searches the log file near the timestamp for a users entries

        Iterates over the log and finds all entries within timedelta of the
        target_timestamp and then prints them to stdout.
    """

    if not os.path.isfile(input_file_name):
        raise Exception("%s does not exist" % input_file_name)

    # Get the most restrictive term to search over
    try:
        grep_search_string = next(t for t in [target_bingo_id,
                                              search_string,
                                              target_url,
                                              target_user_agent,
                                              str(target_status)] if t)
    except StopIteration:
        raise Exception("Search too broad, need at least one of "
                        "target_status, target_bingo_id, target_url, "
                        "target_user_agent, or search_string.  If you want "
                        "the entire file run 'zcat %s'" % input_file_name)

    requests_found = 0

    # Run standard grep on the log files first to more quickly narrow down
    # what we have to search for. We will grab the 60 lines before and
    # after the bingo_id to make sure that we get any tracebacks in the
    # app logs.
    process = subprocess.Popen(['zgrep', '-A', '60', '-B', '60',
                                grep_search_string, input_file_name],
                               stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT)

    with contextlib.closing(process.stdout) as input_file:

        for (request_log_line, request_log_match, app_log_lines) in (
             rlm.RequestLogIterator(input_file)):

            if request_log_match is None:
                # RequestLogIterator encountered the 'sentinel'
                continue

            datetimestamp_string = request_log_match.group("time_stamp")
            # Get the timezone information off the string
            offset = int(datetimestamp_string[-5:])
            tz_delta = datetime.timedelta(hours=offset / 100)

            # Remove the timezone information from the string
            datetimestamp_string = datetimestamp_string[:-6]
            datetimestamp = datetime.datetime.strptime(datetimestamp_string,
                                                       "%d/%b/%Y:%H:%M:%S")

            # Subtract the timezone difference off the date
            datetimestamp = datetimestamp - tz_delta
            timestamp = int(datetime.datetime.strftime(datetimestamp, "%s"))

            # TODO(james): binary search the log file to get to the target
            # start time quickly
            if (timestamp < target_timestamp - timedelta):
                continue

            # TODO(james): sometimes the log file seems out of order by a
            # couple of seconds.  Determine if we want a buffer here.
            if timestamp > target_timestamp:
                break

            if target_status:
                status = request_log_match.group("status")
                if int(status) != target_status:
                    continue

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

            # If we have a search string, continue if it doesn't appear in
            # either the request log line or any of the app_log_lines
            if search_string and not any(search_string in l for l in
                                         [request_log_line] + app_log_lines):
                continue

            if target_url:
                url = request_log_match.group("url")
                # We want to match the referer as well as the bug might have
                # been in an ajax request that was sent from that url.
                referer = request_log_match.group("referer")
                # The target url if coming from a google issue might be stipped
                # of all slashes
                # TODO(james): make sure this is the same stripping that
                # happens in google issue labels
                stripped_url = re.sub("[^\w.]", "", url)
                stripped_referer = re.sub("[^\w.]", "", referer)
                if (target_url != url and
                    target_url != referer and
                    target_url != stripped_url and
                    target_url != stripped_referer):
                    continue

            if target_user_agent:
                # When we don't have a bingo_id, the user_agent might be a way
                # to pseudo identify the bug.
                user_agent = request_log_match.group("user_agent")
                # TODO(james): make sure this is the same stripping that
                # happens in google issue labels
                stripped_user_agent = re.sub("[^\w.]", "", user_agent)
                if (target_user_agent != user_agent and
                    target_user_agent != stripped_user_agent):
                    continue

            requests_found += 1

            # The request_log_line already has a \n at the end of it
            print request_log_line,

            # Tracebacks can have the tabs and newlines escaped. We unescape
            # them here for readability
            print "".join([l.replace('\\t', '\t').replace('\\n', '\n')
                           for l in app_log_lines])

    return requests_found


def find_log_files_for(backends, timestamp, time_delta=DEFAULT_TIME_DELTA):
    """Find the files with logs lines in time_delta seconds before timestamp

    Files on the analytics machine are stored in UTC.
    """
    file_name_format = "%Y/%m/%d/%H:00:00Z.log.gz"
    if backends:
        file_name_format = "%Y/%m/%d/backends-%H:00:00Z.log.gz"

    start_timestamp = timestamp - time_delta
    start_hour_timestamp = start_timestamp - start_timestamp % 3600
    start_hour_datetime = datetime.datetime.fromtimestamp(start_hour_timestamp)
    end_datetime = datetime.datetime.fromtimestamp(timestamp)
    files = []
    current_hour_datetime = start_hour_datetime
    while current_hour_datetime < end_datetime:
        filename = (BASE_DIR +
                    datetime.datetime.strftime(current_hour_datetime,
                                               file_name_format))
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
                            'override any bingo_id retrieved from the issue. '
                            'Default is None.'))
    parser.add_option('--status', '-s', dest='status', type=int,
                      help='Only output logs with this status code. By '
                     'default it does not filter by status')
    parser.add_option('--timestamp', '-t', dest='timestamp', type=int,
                  help=('UNIX timestamp for the end of the time range to look '
                        'at. If none is provided and none is listed in '
                        'the issue then it will look at the end of the '
                        'most recent log that has been fully downloaded.'))
    parser.add_option('--timedelta', '-d', dest='time_delta', type=int,
                      default=DEFAULT_TIME_DELTA,
                      help=('How long before the timestamp you want to search '
                            '(in minutes).  Defaulit is %i minutes' %
                            DEFAULT_TIME_DELTA))
    parser.add_option('--use-url', '-u', dest='use_url', action='store_true',
                      help=('Whether to filter the logs by the url/referer '
                            'url found in the referrer referenced in the '
                            'issue.  By default it won\'t'))
    parser.add_option('--url', '-l', type=str, dest='url',
                      help='Url to filter by. Default is None.')
    parser.add_option('--user-agent', '-a', dest='user_agent', type=str,
                      help=('Filter the log using this user-agent string. '
                            'This will only be used if the bingo_id is not '
                            'set either in the issue or manually with -b. '
                            'Default is None'))
    parser.add_option('--quiet', '-q', dest='quiet', action='store_true',
                      help=('Whether to supress header and footer information '
                            'about the search and the results found. By '
                            'default the header is shown.'))
    parser.add_option('--backends', '-n', dest='backends', action='store_true',
                      help=('Whether to to search backend logs. By default, th'
                        'is setting is False and frontend logs are searched.'),
                        default=False)

    options, args = parser.parse_args()

    search_string = None
    if args:
        search_string = args[0]

    target_url = None
    target_timestamp = None
    target_bingo_id = None
    target_url = None
    target_user_agent = None
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

    time_delta = options.time_delta * 60

    if not target_timestamp:
        # As the last hour full hour's log might not be fully written we will
        # have our stop time be the start of the previous hour.
        now = int(datetime.datetime.strftime(datetime.datetime.now(), "%s"))
        current_hour = now - now % 3600
        target_timestamp = current_hour - 3600

    file_names = find_log_files_for(
        options.backends, int(target_timestamp), time_delta)

    if not options.quiet:
        start_time = time.time()
        conditions = []
        if search_string:
            conditions.append("where it contains '%s'" % search_string)
        if target_bingo_id:
            conditions.append("where the bingo_id = '%s'" % target_bingo_id)
        if target_url:
            conditions.append("where the url or referer = '%s'" % target_url)
        if options.status:
            conditions.append("where the status code = %i" % options.status)
        if target_user_agent:
            conditions.append("where the user_agent = '%s'" %
                              target_user_agent)

        target_datetime = datetime.datetime.fromtimestamp(float(
            target_timestamp))

        print ("Searching %s\nfor all requests in the %i minutes before "
               "%s\n%s\n" % (
                   file_names,
                    options.time_delta,
                    datetime.datetime.strftime(target_datetime,
                                               "%d/%b/%Y:%H:%M:%S"),
                    "\nand ".join(conditions)))

    requests_found = 0
    for file_name in file_names:
        requests_found += parse_log_file(file_name,
                                         target_timestamp,
                                         search_string,
                                         time_delta,
                                         options.status,
                                         target_bingo_id,
                                         target_url,
                                         target_user_agent)
    if not options.quiet:
        end_time = time.time()
        print "Found %i requests matching your conditions in %i seconds" % (
            requests_found, end_time - start_time)
