#!/usr/bin/env python

"""Script that fetches appengine logs from Khan Academy.

This script uses the KA /api/v1/fetch_logs API to fetch logs in the
same format that AppEngine reports the logs itself.  However, this
method is more reliable than using the appengine bulk-download tool to
get logs.

The KA api returns zlib-compressed logs.  We uncompress them and stream
to stdout.

WARNING: AppEngine does not keep logs for all time -- if the
end_time_t is too long ago, then AppEngine will return empty results.
As of 15 May 2012, the limit seems to be about 20 hours in the past.
This value is probably relative to the number of logs present, so will
get smaller as time goes on.  You should run this script frequently.
"""

import datetime
import optparse
import sys
import time
import zlib

import date_util
import oauth_util.fetch_url


LOGS_URL = '/api/v1/fetch_logs/%(start_time_t)s/%(end_time_t)s'


def _read_versions(filename):
    """Look for lines 'appengine_versions: v1,v2,...'; return set(v1,v2,...)"""
    prefix = 'appengine_versions: '
    versions = set()
    try:
        f = open(filename)
        for line in f:
            if line.startswith(prefix):
                versions.update(line[len(prefix):].strip().split(','))
    except (IOError, OSError), why:
        # Errors reading f?  We log, but otherwise just silently continue.
        print >>sys.stderr, ('Could not open alternate-versions file %s: %s'
                             % (filename, why))

    return versions


def _split_into_headers_and_body(loglines_string):
    """Split the fetch_logs output into header lines and log lines.

    hg commit c3887adcec6b added the ability for /api/v1/fetch_logs to
    emit header lines before the actual log-lines.  These header lines
    can include meta-information about the logs being fetched, such as
    the version number of appengine that was active when these logs
    were generated.  We want to log this meta-information to stderr,
    while logging the actual log-lines, which follow the headers, to
    stdout.  This routine helps in that separation, while also
    handling the legacy case that there are no header lines.

    The fetch_logs form is header lines<blank line>log lines.

    Arguments:
       loglines_string: the output of /api/v1/fetch_logs/...

    Returns:
       Two strings: the header lines and the log lines.  Each ends with \n.
    """
    # Old fetch_logs format doesn't have header lines.  So we have to
    # guess if this is old-format output or new-format.  We know all
    # old-format logs start with 0-9 (the first field of those
    # loglines is an IP address), so that's one hint.  Another hint is
    # if there's no blank line in the first X characters (where X is
    # larger than our headers are ever likely to be).  If both are
    # true, we assume there are no headers.
    if loglines_string and loglines_string[0].isdigit():
        pos_after_header_blankline = loglines_string.find('\n\n', 0, 4096)
    else:
        pos_after_header_blankline = loglines_string.find('\n\n')

    if pos_after_header_blankline == -1:    # no blank line found
        return ('', loglines_string)
    return (loglines_string[:pos_after_header_blankline + 1],
            loglines_string[pos_after_header_blankline + 2:])


def _num_requests_in_logs(body):
    """Given a collection of log-lines, return the number of user requests."""
    # We look for text that's likely only in actual loglines, not in
    # logging messages, python stacktraces, or the like.  We look for
    # ' ms='.  ' instance=' or ' cpm_usd=' would also work.  If we're
    # unlucky in a log message, this count may be off by a bit.
    return body.count(' ms=')


def get_cmd_line_args():
    today_start = datetime.datetime.combine(datetime.date.today(),
                                            datetime.time())
    yesterday_start = today_start - datetime.timedelta(days=1)

    parser = optparse.OptionParser(
        usage="%prog [options]",
        description=("Fetches logs from khanacademy.org using its v1 API. "
                     "Outputs to stdout."))

    parser.add_option("-s", "--start_date",
                      default=date_util.to_date_iso(yesterday_start),
                      help=("Earliest inclusive date of logs to fetch, "
                            "in ISO 8601 format. "
                            "Defaults to yesterday at 00:00."))
    parser.add_option("-e", "--end_date",
                      default=date_util.to_date_iso(today_start),
                      help=("Latest exclusive date of logs to fetch, "
                            "in ISO 8601 format. "
                            "Defaults to today at 00:00."))
    parser.add_option("-i", "--interval", default=10,
                      help=("Time interval to fetch at a time, in seconds. "
                            "Defaults to 10."))
    parser.add_option("-r", "--max_retries", default=8,
                      help=("Maximum # of retries for request attempts "
                            "before failing. Defaults to 8."))
    parser.add_option("-v", "--appengine_version", default=None,
                      help=("If set, the appengine-version (e.g. "
                            "0515-ae96fc55243b) to request the logs from. "
                            "If None, will use the current active version."))
    parser.add_option("--file_for_alternate_appengine_versions", default=None,
                      help=("If set, whenever fetching logs returns 0 "
                            "loglines, look through this file to find "
                            "alternate appengine_versions, and retry "
                            "fetching the logs against those versions "
                            "until one returns non-zero results.  The "
                            "file should have text like "
                            "appengine_versions=v1,v2,v3"))
    #TODO: Figure out a better way to get the backend versions that were active
    #during the time period.
    parser.add_option("-b", "--backend", action="store_true", default=False,
                      help=("If set will try and read the logs from the "
                            "versions in the file designated by "
                            "--file_for_alternate_appengine_versions with "
                            "'mapreducebackends-' prefixed onto them. If "
                            "there is no file, then no versions will be "
                            "searched"))

    options, extra_args = parser.parse_args()
    if extra_args:
        sys.exit('Unknown arguments %s. See --help.' % extra_args)
 
    return options


def fetch_appengine_logs(start_time, end_time, appengine_versions):
    """Return the output from /api/v1/fetch_logs.

    Arguments:
      start_time: a datetime object saying when to start fetching from.
      end_time: a datetime object saying when to stop fetching.
      appengine_versions: an ordered list (or tuple).  First, we tell
        appengine to retrieve the logs from the first
        appengine-version in the list.  If appengine can't find any
        logs in that time period corresponding to that version, we'll
        try the next version in the list.  If none of them ever give
        any output, we return the empty string.  If appengine_versions
        is the empty list, we just try once, telling appengine to use
        the current default (live) appengine version.

    Returns:
      A string, the output of the /api/v1/fetch_logs/x/x?... command.
    """
    start_time_t = int(time.mktime(start_time.timetuple()))
    end_time_t = int(time.mktime(end_time.timetuple()))
    url_base = LOGS_URL % {'start_time_t': start_time_t,
                           'end_time_t': end_time_t}

    if not appengine_versions:
        return oauth_util.fetch_url.fetch_url(url_base)

    for (i, appengine_version) in enumerate(appengine_versions):
        if appengine_version is None:    # None means 'use the default'
            url = url_base
        else:
            url = url_base + '?appengine_version=%s' % appengine_version
        compressed_retval = oauth_util.fetch_url.fetch_url(url)
        retval = zlib.decompress(compressed_retval)
        (_, loglines_as_string) = _split_into_headers_and_body(retval)
        if loglines_as_string:
            return retval

        print >>sys.stderr, ('No logs for version %s, trying version %s'
                             % (appengine_version or '[default]',
                                (appengine_versions + ['<giving up>'])[i + 1]))

    return ''      # We never found a non-empty body, so just bail.


def main():
    """Returns the number of fetches that resulted in an error."""
    options = get_cmd_line_args()

    start_dt = date_util.from_date_iso(options.start_date)
    end_dt = date_util.from_date_iso(options.end_date)
    interval = int(options.interval)
    max_retries = int(options.max_retries)
    appengine_versions = [options.appengine_version]
    if options.file_for_alternate_appengine_versions:
        appengine_versions.extend(
            _read_versions(options.file_for_alternate_appengine_versions))

    if options.backend:
        # there's no 'default' version for backends
        appengine_versions.remove(None) 
        appengine_versions = ["mapreducebackend-" + v 
                              for v in appengine_versions]
        
    print >>sys.stderr, ('Looking at these appengine versions: %s'
                         % [v or '(default)' for v in appengine_versions])

    num_errors = 0
    while start_dt < end_dt:
        next_dt = min(start_dt + datetime.timedelta(seconds=interval), end_dt)

        print >>sys.stderr, '[%s] Fetching logs from [%s, %s)...' % (
            datetime.datetime.now(), start_dt, next_dt)

        for tries in xrange(max_retries):
            try:
                response = fetch_appengine_logs(start_dt, next_dt,
                                                appengine_versions)
            except Exception, why:
                sleep_secs = 2 ** tries
                print >>sys.stderr, ('ERROR: %s.\n'
                                     'Retrying in %s seconds...'
                                     % (why, sleep_secs))
                time.sleep(sleep_secs)
            else:
                # The 'header' portion of the response goes into the
                # fetch-log.  The rest goes into the actual logs.
                (headers, body) = _split_into_headers_and_body(response)
                sys.stderr.write(headers)
                # It's nice to give a brief summary of what the logs are like.
                print >>sys.stderr, ('%s request lines found'
                                     % _num_requests_in_logs(body))
                if not body:
                    print >>sys.stderr, 'WARNING: No logs found'
                print body,
                break
        else:  # for/else: if we get here, we never succeeded in fetching
            num_errors += 1
            print >>sys.stderr, ('SKIPPING logs from %s to %s: error fetching.'
                                 % (start_dt, next_dt))

        start_dt = next_dt


if __name__ == '__main__':
    num_errors = main()
    # Only return 1-127 on error, since 128+ means 'died with signal'
    sys.exit(min(num_errors, 127))
