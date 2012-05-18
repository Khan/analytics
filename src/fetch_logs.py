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

import oauth_util.fetch_url


LOGS_URL = '/api/v1/fetch_logs/%(start_time_t)s/%(end_time_t)s'


def from_date_iso(s_date):
    """Parse date in our approved ISO 8601 format."""
    return datetime.datetime.strptime(s_date, "%Y-%m-%dT%H:%M:%SZ")


def to_date_iso(date):
    datestring = date.isoformat()
    idx = datestring.rfind('.')
    if idx != -1:
        datestring = datestring[:idx]
    return "%sZ" % datestring


def fetch_appengine_logs(start_time, end_time, appengine_version):
    """start_time and end_time should be datetimes."""
    start_time_t = int(time.mktime(start_time.timetuple()))
    end_time_t = int(time.mktime(end_time.timetuple()))

    response_url = LOGS_URL % {'start_time_t': start_time_t,
                               'end_time_t': end_time_t}
    if appengine_version:
        response_url += '?appengine_version=%s' % appengine_version
    return oauth_util.fetch_url.fetch_url(response_url)


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
    if loglines_string[0].isdigit():
        pos_after_header_blankline = loglines_string.find('\n\n', 0, 4096)
    else:
        pos_after_header_blankline = loglines_string.find('\n\n')

    if pos_after_header_blankline == -1:    # no blank line found
        return ('', loglines_string)
    return (loglines_string[:pos_after_header_blankline+1],
            loglines_string[pos_after_header_blankline+2:])


def get_cmd_line_args():
    today_start = datetime.datetime.combine(datetime.date.today(),
                                            datetime.time())
    yesterday_start = today_start - datetime.timedelta(days=1)

    parser = optparse.OptionParser(
        usage="%prog [options]",
        description=("Fetches logs from khanacademy.org using its v1 API. "
                     "Outputs to stdout."))

    parser.add_option("-s", "--start_date",
                      default=to_date_iso(yesterday_start),
                      help=("Earliest inclusive date of logs to fetch, "
                            "in ISO 8601 format. "
                            "Defaults to yesterday at 00:00."))
    parser.add_option("-e", "--end_date",
                      default=to_date_iso(today_start),
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
                            "0515-ae96fc55243b) to request the logs from."))
    options, extra_args = parser.parse_args()
    if extra_args:
        sys.exit('This script takes no arguments!')

    return options


def main():
    """Returns the number of fetches that resulted in an error."""
    options = get_cmd_line_args()

    start_dt = from_date_iso(options.start_date)
    end_dt = from_date_iso(options.end_date)
    interval = int(options.interval)
    max_retries = int(options.max_retries)
    appengine_version = options.appengine_version

    num_errors = 0
    while start_dt < end_dt:
        next_dt = min(start_dt + datetime.timedelta(seconds=interval), end_dt)

        print >>sys.stderr, '[%s] Fetching logs from [%s, %s)...' % (
            datetime.datetime.now(), start_dt, next_dt)

        for tries in xrange(max_retries):
            try:
                compressed_response = fetch_appengine_logs(start_dt, next_dt,
                                                           appengine_version)
                response = zlib.decompress(compressed_response)
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
