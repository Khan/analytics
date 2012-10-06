#!/usr/bin/env python

"""A Hadoop Streaming mapper that formats website logs for Hive

Raw logs are downloaded from the website handler api/v1_fetch_logs.py
and then passed through this filter.

Input is the raw logs, of which some lines may be request logs in the
following format:

A user-facing request:

91.174.232.10 - chris [24/Jul/2012:17:00:09 -0700] "GET /assets/images/thumbnails/Rothko-13.jpg HTTP/1.1" 200 572 "http://smarthistory.khanacademy.org/" "Mozilla/5.0" "smarthistory.khanacademy.org" ms=65 cpu_ms=35 api_cpu_ms=10 cpm_usd=0.000001 pending_ms=0 instance=00c61b117c5f1f26699563074cdd44e841096e

A task queue request, initiated by App Engine:

0.1.0.2 - - [31/Jul/2012:17:00:09 -0700] "POST /_ah/queue/deferred_problemlog HTTP/1.1" 200 84 "http://www.khanacademy.org/api/v1/user/exercises/converting_between_point_slope_and_slope_intercept/problems/1/attempt" "AppEngine-Google; (+http://code.google.com/appengine)" "www.khanacademy.org" ms=88 cpu_ms=300 api_cpu_ms=275 cpm_usd=0.000007 queue_name=problem-log-queue task_name=10459794276075119660 pending_ms=0 instance=00c61b117cca420ac067602b717789e7aec8ca


Output is one record per line. Each record contains tab-separated
values whose fields are, in order with examples from the above logs:

  ip              # 91.174.232.10
  user            # chris
  timestamp       # 24/Jul/2012:17:00:09 -0700
  method          # GET 
  url             # /assets/images/thumbnails/Rothko-13.jpg
  protocol        # HTTPS/1.1
  status code     # 200
  bytes           # 572
  referer         # http://smarthistory.khanacademy.org
  ms              # 65
  cpu_ms          # 35
  api_cpu_ms      # 10
  cpm_usd         # 0.000001
  queue_name      # problem-log-queue (or the empty string if n/a)
  pending_ms      # 0
  url_route       # (an identifier that rolls up multiple similar URLs,
                  #  e.g., video page URLs)

These fields could also be extracted from the logs but aren't because they're
big and we can't figure out how they would be useful. If they do turn out to be
useful, modify this script to capture and output them, and modify the Hive
table definition to contain them:

  user agent      # Mozilla/5.0
  host            # smarthistory.khanacademy.org
  task_name       # 131477025630677193 (or the empty string if n/a)
  instance        # 00c61b117c5f1f26699563074cdd44e841096e
"""

import re
import sys

# This regex matches the Apache combined log format, plus some special
# fields that are specific to App Engine.
_LOG_MATCHER = re.compile(r"""
    ^(\S+)\s                      # IP
    \S+\s                         # remote logname (ignored)
    (\S+)\s                       # remote user
    \[([^]]+)\]\s                 # timestamp
    "(\S+)\s                      # method
     (\S+)\s                      # URL
     ([^"]+)"\s                   # protocol
    (\S+)\s                       # status code
    (\S+)\s                       # bytes
    "([^"\\]*(?:\\.[^"\\]*)*)"\s  # referer
    "[^"\\]*(?:\\.[^"\\]*)*"\s    # user agent (ignored)

    # Apache combined log format is above, custom fields are below.

    "[^"]+"\s                     # host (ignored)
    ms=(\d+)\s
    cpu_ms=(\d+)\s
    api_cpu_ms=(\d+)\s
    cpm_usd=(\S+)\s
    (?:queue_name=(\S+)\s)?
    (?:task_name=\S+\s)?          # (ignored)
    pending_ms=(\d+)\s
    instance=\S+$                 # (ignored)
""", re.X)


def url_route(url):
    # TODO(chris): implement
    return ''


def main():
    for line in sys.stdin:
        match = _LOG_MATCHER.match(line)
        if not match:
            # Ignore non-request logs. It's possible, but unlikely,
            # that we'll get a false positive: something that looks
            # like a request log but was written to the application
            # logs during a request.
            continue

        if '\t' in line:
            raise RuntimeError(
                'The output to Hive is tab-separated. Field values must not '
                'contain tabs, but this log does: %s' % line)

        groups = [(g or "") for g in match.groups()]
        groups.append(url_route(groups[4]))
        print '\t'.join(groups)


if __name__ == '__main__':
    run_tests = False
    if run_tests:
        import doctest
        doctest.testmod()
    else:
        main()
