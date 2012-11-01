#!/usr/bin/env python

"""A Hadoop Streaming mapper that formats website logs for Hive

Raw logs are downloaded from the website handler api/v1_fetch_logs.py
and then passed through this filter.

This script requires a file called route_map_file.json exist in a
place that Elastic Mapreduce can find it (specify the cacheFile
argument as a commandline flag to EMR).  This file should hold the
output of a call to
  http://www.khanacademy.org/stats/route_map?key=<sleep_key_from_secrets.py>

Stdin should hold the raw logs, of which some lines may be request
logs in the following format:

A user-facing request:

91.174.232.10 - chris [24/Jul/2012:17:00:09 -0700] "GET /assets/images/thumbnails/Rothko-13.jpg HTTP/1.1" 200 572 "http://smarthistory.khanacademy.org/" "Mozilla/5.0" "smarthistory.khanacademy.org" ms=65 cpu_ms=35 cpm_usd=0.000001 pending_ms=0 instance=00c61b117c5f1f26699563074cdd44e841096e

A homagepage request, with no referer:
68.202.49.17 - - [29/Oct/2012:17:00:09 -0700] "GET / HTTP/1.1" 200 11377 - "Mozilla/5.0 (Windows NT 6.0; WOW64) AppleWebKit/537.4 (KHTML, like Gecko) Chrome/22.0.1229.94 Safari/537.4" "www.khanacademy.org" ms=200 cpu_ms=103 cpm_usd=0.000001 pending_ms=0 instance=00c61b117c4811eae292cd1ee62739468526

A task queue request, initiated by App Engine:

0.1.0.2 - - [31/Jul/2012:17:00:09 -0700] "POST /_ah/queue/deferred_problemlog HTTP/1.1" 200 84 "http://www.khanacademy.org/api/v1/user/exercises/converting_between_point_slope_and_slope_intercept/problems/1/attempt" "AppEngine-Google; (+http://code.google.com/appengine)" "www.khanacademy.org" ms=88 cpu_ms=300 cpm_usd=0.000007 queue_name=problem-log-queue task_name=10459794276075119660 pending_ms=0 instance=00c61b117cca420ac067602b717789e7aec8ca

Additional logs for testing. Note that instance may be "None",
and cpm_usd is occasionally, unexpectedly, 0.000000:

122.11.36.130 - - [06/Oct/2012:16:00:09 -0700] "GET /api/v1/user/topic/precache/addition-subtraction/e/addition_1?casing=camel HTTP/1.1" 200 2421 "http://www.khanacademy.org/math/arithmetic/addition-subtraction/v/basic-addition" "Mozilla/5.0 (iPad; CPU OS 5_1_1 like Mac OS X) AppleWebKit/534.46 (KHTML, like Gecko) Version/5.1 Mobile/9B206 Safari/7534.48.3" "www.khanacademy.org" ms=263 cpu_ms=415 cpm_usd=0.000000 pending_ms=0 instance=00c61b117c29add96b8c86824f3be8d9b22d5537

174.211.15.119 - - [06/Oct/2012:16:00:09 -0700] "GET /images/featured-actions/campbells-soup.png HTTP/1.1" 204 154518 "http://www.khanacademy.org/" "Mozilla/5.0 (iPhone; CPU iPhone OS 5_0_1 like Mac OS X) AppleWebKit/534.46 (KHTML, like Gecko) Version/5.1 Mobile/9A405 Safari/7534.48.3" "www.khanacademy.org" ms=19 cpu_ms=0 cpm_usd=0.000017 pending_ms=0 instance=None


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

import json
import re
import sys
import urlparse


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
    (?:"(?P<referer>[^"\\]*(?:\\.[^"\\]*)*)"|-)\s
    "(?P<user_agent>[^"\\]*(?:\\.[^"\\]*)*)"\s

    # Apache combined log format is above, custom fields are below.

    "(?P<host>[^"]+)"\s
    ms=(?P<ms>\d+)\s
    cpu_ms=(?P<cpu_ms>\d+)\s
    # Old logs (before Oct 2012) used to have an api_cpu_ms field.
    (?:api_cpu_ms=\S+\s)?
    cpm_usd=(?P<cpm_usd>\S+)\s
    (?:queue_name=(?P<queue_name>\S+)\s)?
    (?:task_name=(?P<task_name>\S+)\s)?
    pending_ms=(?P<pending_ms>\d+)\s
    instance=(?P<instance>\S+)$
""", re.X)


# We emit just these fields, and in this order.
_FIELDS_TO_KEEP = ('ip', 'user', 'time_stamp', 'method', 'url',
                   'protocol', 'status', 'bytes', 'referer',
                   'ms', 'cpu_ms', 'cpm_usd', 'queue_name',
                   'pending_ms')


def url_route(method, url, route_regexps):
    """Determine the regexp that would be matched to handle this url.

    This looks at the wsgi apps (including the flask wsgi app) to
    determine what pattern matches the url, which in turn can be used
    to determine what handler will handle the url.

    This is used to group urls together.  For instance, we want all
    requests that look like /.*/e to be grouped together, since
    they're all the 'same' request, in that they're handled by the
    same handler.  (In fact, this method could return the handler, but
    we decided returning the regexp is more human-readable.)

    Arguments:
        method: the method of the request: 'GET', 'POST', etc.
        url: the full url of the request, including protocol, hostname,
            query, etc.
        route_regexps: An iterable that returns
               (app_yaml_regexp, wsgi_regexp, list-of-methods)
            tuples (one for each route in our app).  app_yaml_regexp
            is the regexp in app.yaml that specifies the wsgi app,
            and wsgi_regexp is the regexp in the wsgi app (main.py,
            e.g.) that specifies the handler to run.  We return a
            unique representation of the first regexp that matches
            the given method + url, or the path-component of the url
            itself if none does.  list-of-methods can be the empty
            list to indicate that *all* methods match.

    Returns:
        A string representation of the matching regexp that is enough
        to uniquely identify the regexp.  Normally this will be
        wsgi_regexp (a regexp found in main.py or api/main.py).
        However, if the same wsgi_regexp is found in two places
        in route_regexps -- they'll have different app_yaml_regexps
        -- we'll distinguish them by returning
        '(<app_yaml_regexp>)(<wsgi_regexp>)'.

        The 'matching regexp' is calculated as follows: find the
        first app_yaml_regexp that matches the url, and then find
        the first wsgi_regexp with that app_yaml_regexp that matches.

        If no matching regexp is found, return the input url-path.
    """
    url_path = urlparse.urlparse(url).path
    matched_app_yaml_regexp = None
    for (app_yaml_regexp, wsgi_regexp, methods) in route_regexps:
        if (matched_app_yaml_regexp is None
            and app_yaml_regexp.search(url_path)):
            matched_app_yaml_regexp = app_yaml_regexp

        # Now check if the wsgi regexp is a match as well.
        if ((not methods or method in methods) and  # method matches
            app_yaml_regexp == matched_app_yaml_regexp and
            wsgi_regexp.search(url_path)):          # wsgi regexp matches
            matched_wsgi_regexp = wsgi_regexp
            break
    else:     # for/else: if we get here, no regexp matched
        # This should never hit since we have catch-all urls in
        # app.yaml and main.py.  It's just for safety.
        return url_path

    # Now check if wsgi_regexp is unique, and we can just return it,
    # or we need to include app_yaml_regexp too.
    is_duplicated = False
    for (app_yaml_regexp, wsgi_regexp, methods) in route_regexps:
        if (wsgi_regexp == matched_wsgi_regexp and
            app_yaml_regexp != matched_app_yaml_regexp):
            is_duplicated = True
            break

    if is_duplicated:
        return '(%s)(%s)' % (matched_app_yaml_regexp.pattern,
                             matched_wsgi_regexp.pattern)
    else:
        return matched_wsgi_regexp.pattern


def convert_stats_route_map_to_route_regexps(route_map_json):
    """Convert the output of ka.org/stats/route_map to url_route input."""
    route_regexps = []
    for app_yaml_info in route_map_json:
        app_yaml_regexp = re.compile(app_yaml_info[0])
        # We skip app_yaml_info[1], the wsgi app name.
        for wsgi_info in app_yaml_info[2:]:
            wsgi_regexp = re.compile(wsgi_info[0])
            # We skip wsgi_info[1], the handler name
            methods = wsgi_info[2:]
            route_regexps.append((app_yaml_regexp, wsgi_regexp, methods))
    return route_regexps


def main(input_file, route_regexps):
    """Print a converted logline for each logline in input_file.

    Also adds a few derived fields, such as the url_route (the
    wsgi route that this url tickled).

    Arguments:
        input_file: a file containing loglines as taken from appengine.
        route_regexps: An iterable that returns
            (app_yaml_regexp, wsgi_regexp, list-of-methods),
            and is passed to url_route().
    """
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

        fields = {}
        for f in _FIELDS_TO_KEEP:
            fields[f] = match.group(f) or ''

        # Get a copy of the field-values in _FIELDS_TO_KEEP order.
        sorted_fields = sorted(fields.items(),
                               key=lambda kv: _FIELDS_TO_KEEP.index(kv[0]))

        # -- Now we add derived fields.

        # Map the URL to its route.
        sorted_fields.append(('url_route',
                              url_route(fields['method'], fields['url'],
                                        route_regexps)))

        print '\t'.join(v for (k, v) in sorted_fields)


if __name__ == '__main__':
    with open('route_map_file.json') as f:
        route_regexps = convert_stats_route_map_to_route_regexps(json.load(f))

    main(sys.stdin, route_regexps)
