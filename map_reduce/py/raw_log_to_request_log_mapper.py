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

Additional logs for testing. Note that api_cpu_ms and instance may be "None",
and cpm_usd is occasionally, unexpectedly, 0.000000:

122.11.36.130 - - [06/Oct/2012:16:00:09 -0700] "GET /api/v1/user/topic/precache/addition-subtraction/e/addition_1?casing=camel HTTP/1.1" 200 2421 "http://www.khanacademy.org/math/arithmetic/addition-subtraction/v/basic-addition" "Mozilla/5.0 (iPad; CPU OS 5_1_1 like Mac OS X) AppleWebKit/534.46 (KHTML, like Gecko) Version/5.1 Mobile/9B206 Safari/7534.48.3" "www.khanacademy.org" ms=263 cpu_ms=415 api_cpu_ms=None cpm_usd=0.000000 pending_ms=0 instance=00c61b117c29add96b8c86824f3be8d9b22d5537

174.211.15.119 - - [06/Oct/2012:16:00:09 -0700] "GET /images/featured-actions/campbells-soup.png HTTP/1.1" 204 154518 "http://www.khanacademy.org/" "Mozilla/5.0 (iPhone; CPU iPhone OS 5_0_1 like Mac OS X) AppleWebKit/534.46 (KHTML, like Gecko) Version/5.1 Mobile/9A405 Safari/7534.48.3" "www.khanacademy.org" ms=19 cpu_ms=0 api_cpu_ms=None cpm_usd=0.000017 pending_ms=0 instance=None


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

import collections
import re
import sys

# This regex matches the Apache combined log format, plus some special
# fields that are specific to App Engine.  The names used for the regexp
# groups match the names used in the hive table in ../hive/ka_hive_init.q
_LOG_MATCHER = re.compile(r"""
    ^(?P<ip>\S+)\s
    (?P<logname>\S+)\s
    (?P<user>\S+)\s
    \[(?P<time_stamp>[^]]+)\]\s
    "(?P<method>\S+)\s
     (?P<url>\S+)\s
     (?P<protocol>[^"]+)"\s
    (?P<status>\S+)\s
    (?P<bytes>\S+)\s
    "(?P<referer>[^"\\]*(?:\\.[^"\\]*)*)"\s
    "(?P<user_agent>[^"\\]*(?:\\.[^"\\]*)*)"\s

    # Apache combined log format is above, custom fields are below.

    "(?P<host>[^"]+)"\s
    ms=(?P<ms>\d+)\s
    cpu_ms=(?P<cpu_ms>\d+)\s
    api_cpu_ms=(?P<api_cpu_ms>\d+|None)\s
    cpm_usd=(?P<cpm_usd>\S+)\s
    (?:queue_name=(?P<queue_name>\S+)\s)?
    (?:task_name=(?P<task_name>\S+)\s)?
    pending_ms=(?P<pending_ms>\d+)\s
    instance=(?P<instance>\S+)$
""", re.X)


_FIELDS_TO_KEEP = ('ip', 'user', 'time_stamp', 'method', 'url',
                   'protocol', 'status', 'bytes', 'referer',
                   'ms', 'cpu_ms', 'api_cpu_ms', 'cpm_usd', 'queue_name',
                   'pending_ms')


def url_route(url):
    # TODO(chris): implement
    return ''


def main(input_file):
    for line in input_file:
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

        fields = collections.OrderedDict()
        for f in _FIELDS_TO_KEEP:
            fields[f] = match.group(f) or ''

        # api_cpu_ms may be "None" if not provided by App Engine. The
        # equivalent in Hive is an empty field.
        if fields['api_cpu_ms'] == 'None':
            fields['api_cpu_ms'] = ''

        # Now we add derived fields.

        # Map the URL to its route.
        fields['url_route'] = url_route(fields['url'])

        print '\t'.join(fields.itervalues())


if __name__ == '__main__':
    main(sys.stdin)
