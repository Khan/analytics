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
68.202.49.17 - - [29/Oct/2012:17:00:09 -0700] "GET / HTTP/1.1" 200 11377 - "Mozilla/5.0 (Windows NT 6.0; WOW64) AppleWebKit/537.4 (KHTML, like Gecko) Chrome/22.0.1229.94 Safari/537.4" "www.khanacademy.org" ms=200 cpu_ms=103 cpm_usd=0.000001 pending_ms=0 instance=00c61b117c4811eae292cd1ee62739468526  @Nolint

A task queue request, initiated by App Engine:

0.1.0.2 - - [31/Jul/2012:17:00:09 -0700] "POST /_ah/queue/deferred_problemlog HTTP/1.1" 200 84 "http://www.khanacademy.org/api/v1/user/exercises/converting_between_point_slope_and_slope_intercept/problems/1/attempt" "AppEngine-Google; (+http://code.google.com/appengine)" "www.khanacademy.org" ms=88 cpu_ms=300 cpm_usd=0.000007 queue_name=problem-log-queue task_name=10459794276075119660 pending_ms=0 instance=00c61b117cca420ac067602b717789e7aec8ca

Additional logs for testing. Note that instance may be "None",
and cpm_usd is occasionally, unexpectedly, 0.000000:

122.11.36.130 - - [06/Oct/2012:16:00:09 -0700] "GET /api/v1/user/topic/precache/addition-subtraction/e/addition_1?casing=camel HTTP/1.1" 200 2421 "http://www.khanacademy.org/math/arithmetic/addition-subtraction/v/basic-addition" "Mozilla/5.0 (iPad; CPU OS 5_1_1 like Mac OS X) AppleWebKit/534.46 (KHTML, like Gecko) Version/5.1 Mobile/9B206 Safari/7534.48.3" "www.khanacademy.org" ms=263 cpu_ms=415 cpm_usd=0.000000 pending_ms=0 instance=00c61b117c29add96b8c86824f3be8d9b22d5537

174.211.15.119 - - [06/Oct/2012:16:00:09 -0700] "GET /images/featured-actions/campbells-soup.png HTTP/1.1" 204 154518 "http://www.khanacademy.org/" "Mozilla/5.0 (iPhone; CPU iPhone OS 5_0_1 like Mac OS X) AppleWebKit/534.46 (KHTML, like Gecko) Version/5.1 Mobile/9A405 Safari/7534.48.3" "www.khanacademy.org" ms=19 cpu_ms=0 cpm_usd=0.000017 pending_ms=0 instance=None

Other lines in stdin hold the application logs, of which a maximum of one may
be a KALOG in the format KALOG;(key(:value)?;)*

ie:
        0:1356120009.94 KALOG;pageload;testempty:;bingo.param:scratchpad_all_bi
nary;bingo.param:scratchpad_all_count;id.bingo:_gae_bingo_random%3AL4oWrCzmmgZb
7UAbCtT7T9NzFQKVQ5MhlJpE4uBo;

Output is one record per request log. Each record contains tab-separated
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
  bingo_id        # _gae_bingo_random:L4oWrCzmmgZb7UAbCtT7T9NzFQKVQ5MhlJpE4uBo
                  # (the bingo_id found in a KALOG within the applogs following
                  # the request log)
  kalog           # the full kalog line

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
    \[(?P<time_stamp>[^\]]+)\]\s
    "(?P<method>\S+)\s
     (?P<url>\S+)\s
     (?P<protocol>[^"]+)"\s
    (?P<status>\S+)\s
    (?P<bytes>\S+)\s
    (?:"(?P<referer>[^"\\]*(?:\\.[^"\\]*)*)"|-)\s
    (?:"(?P<user_agent>[^"\\]*(?:\\.[^"\\]*)*)"|-)\s

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

_KA_LOG_MATCHER = re.compile(r"""
    ^\s*[\d.\:]*\s*
    KALOG;(?P<keyvalues>(?:[^\:;]+(?:\:[^;]*)?;)*)
    id.bingo:(?P<bingo_id>[^;]+);$
""", re.X) 


def route_for_url(route_map, url, method):
    """Return the wsgi route for a given url + method.

    This looks at the wsgi apps (including the flask wsgi app) to
    determine what pattern matches the url, which in turn can be used
    to determine what handler will handle the url.

    This is used to group urls together.  For instance, we want all
    requests that look like /.*/e to be grouped together, since
    they're all the 'same' request, in that they're handled by the
    same handler.  (In fact, this method could return the handler, but
    we decided returning the regexp is more human-readable.)

    Arguments:
        route_map: A list of app-yaml and wsgi-regexps, as returned
            by route_map.py:generate_route_map(), or by
            http://www.khanacademy.org/stats/route_map (but with the
            regexp strings converted to actual regexps).
        url: the full url of the request, including protocol, hostname,
            query, etc.
        method: the method of the request: 'GET', 'POST', etc.

    Returns:
        A string representation of the matching regexp that is enough
        to uniquely identify the regexp, along with the 'module' where
        the wsgi app is from.

        The 'matching regexp' is calculated as follows: find the
        first app_yaml_regexp that matches the url, and then find
        the first wsgi_regexp with that app_yaml_regexp that matches.

        If no matching regexp is found, return the input url-path.
    """
    url_path = urlparse.urlparse(url).path

    # The below is copied (ugh) from ka-stable:route_map.py:route_for_url()
    for app_yaml_info in route_map:
        app_yaml_regexp = app_yaml_info[0]
        app_yaml_module = re.sub(r'\.[^.]+$', '', app_yaml_info[1])
        if app_yaml_regexp.search(url_path):
            break
    else:   # for/else
        # No match in app.yaml?  It's a 404, which we indicate by
        # returning the input unchanged.
        return url_path

    for wsgi_info in app_yaml_info[2:]:
        wsgi_regexp = wsgi_info[0]
        methods = wsgi_info[2:]
        if ((not methods or method in methods) and  # method matches
            wsgi_regexp.search(url_path)):     # wsgi regexp matches
            break
    else:  # for/else
        # No match in the wsgi route table?  Also a 404.
        return url_path

    regexp = wsgi_regexp.pattern
    # Clean up the regexp a little bit.
    if regexp.startswith('^') and regexp.endswith('$'):
        regexp = regexp[1:-1]
    regexp = regexp.replace(r'\/', '/')
    # TODO(csilvers): 'canonicalize' the method for flask requests?
    if method != 'GET':
        regexp += ' [%s]' % method
    return '%s:%s' % (app_yaml_module, regexp)


def convert_stats_route_map_strings_to_regexps(route_map):
    """Convert re-strings in ka.org/stats/route_map output to re objects."""
    for app_yaml_info in route_map:
        app_yaml_info[0] = re.compile(app_yaml_info[0])
        # We skip app_yaml_info[1], the wsgi app name.
        for wsgi_info in app_yaml_info[2:]:
            wsgi_info[0] = re.compile(wsgi_info[0])


class RequestLogIterator:
    """Iterates over each logline together with its app logs.

    Performs a match on each request_log and returns a tuple of the line, the
    match, and all subsequent lines that don't match a request_log (ie. the app
    logs)
    """

    sentinel = object()  # Used to mark the end of the file

    def __init__(self, input_file):
        self._iter = iter(input_file)
        self._set_next_line()
        
    def __iter__(self):
        return self

    def _set_next_line(self):
        try:
            self.next_line = self._iter.next()
            self.next_match = _LOG_MATCHER.match(self.next_line)
        except StopIteration:
            self.next_line = self.sentinel
            self.next_match = None

    def next(self):
        request_log_line = self.next_line
        request_log_match = self.next_match
        if request_log_line == self.sentinel:
            raise StopIteration

        app_log_lines = []
        while True: 
            self._set_next_line()
            if (self.next_line == self.sentinel or self.next_match):
                return (request_log_line, request_log_match, app_log_lines)
            else:
                app_log_lines.append(self.next_line)


def main(input_file, route_map):
    """Print a converted logline for each request logline in input_file.

    Also adds a few derived fields, such as the url_route (the
    wsgi route that this url tickled) and the bingo_id and other key_values
    from the ka_log

    Arguments:
        input_file: a file containing loglines as taken from appengine.
        route_map: A list of app-yaml and wsgi-regexps, as returned
            by route_map.py:generate_route_map(), or by
            http://www.khanacademy.org/stats/route_map (but with the
            regexp strings converted to actual regexps).  This is
            passed as-is to route_for_url().
    """

    for (request_log_line, request_log_match, app_log_lines) in (
         RequestLogIterator(input_file)):

        if '\t' in request_log_line:
            raise RuntimeError(
                'The output to Hive is tab-separated. Field values must not '
                'contain tabs, but this log does: %s' % request_log_line)

        sorted_fields = [(f, request_log_match.group(f) or '') 
                         for f in _FIELDS_TO_KEEP]
        # -- Now we add derived fields.

        # Map the URL to its route.
        sorted_fields.append(('url_route',
                              route_for_url(route_map,
                                  request_log_match.group('url'),
                                  request_log_match.group('method'))))

        # Add the bingo_id and kalog if it exists in the app logs
        for line in app_log_lines:
            kalog_match = _KA_LOG_MATCHER.match(line)
            if kalog_match:
                
                # We extract out the bingo_id and unquote it now for ease of 
                # searching the lines by bingo_id (note bingo_ids will still
                # be put into hive urllib quoted.
                sorted_fields.append(('bingo_id', 
                                      kalog_match.group("bingo_id")))
                sorted_fields.append(('kalog', 
                                      kalog_match.group("keyvalues")))

                # There seems to be a bug that very occassionally the kalog
                # line gets duplicated such as on  07/Jan/2013:18:06:09
                # There could also be second kalog in app_log_lines if the 
                # request_log_matcher fails to match a true request log in
                # which case we will ignore the second one
                break

        print '\t'.join(v for (k, v) in sorted_fields)

if __name__ == '__main__':
    with open('route_map_file.json') as f:
        route_map = json.load(f)
        convert_stats_route_map_strings_to_regexps(route_map)

    main(sys.stdin, route_map)
