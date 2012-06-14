#!/usr/bin/env python

"""Analyze the data produced by the memcache logging in memcache_with_stats.py.

memcache_with_stats logs every memcache access (or, optionally, a
fraction of all memcache accesses, selected based on the key).  It
then logs lines like this:
   memcache: get "mynamespace:mykey" (fail)
   memcache: set "mynamespace:mykey" (904 bytes) (expires 2160000)

This script parses out these lines from the logfile and does some ad-hoc
analysis on them.
"""

import gzip
import re
import sys
import time


# This ignores the timezone (-0700) because time.strptime can't parse
# it.  This means all time-t's in the request object will be off by 7
# hours, but the relative times will still be right.
_REQUEST_LINE_RE = re.compile(r'[\d.]+ \S+ \S+ '
                              r'\[(?P<time>[^]]+) [+-]\d{4}\] '
                              r'"(?P<url>[^"]+)".*')

_REQUEST_LINE_DATE_FORMAT = '%d/%b/%Y:%H:%M:%S'

_MEMCACHE_LINE_RE = re.compile(r'\t1:(?P<time_t>[\d.]+) memcache: '
                               r'(?P<command>[^ ]+) (?P<key>"[^"]+") '
                               r'(?P<success>\(success\)|\(fail\))?\s*'
                               r'(?P<num_bytes>\(\d+ bytes\))?\s*'
                               r'(?P<expires>\(expires \d+\))?\s*'
                               )

# These are strings that are the 'last unique identifier' for a key.
# That is, there are lots of keys that look like
# 'last_action_cache_2_foo', where the foo is different for each user,
# say.  Likewise, there are lots of flags that look like
# 'something_ag5foo' or 'somethingelse_ag5foo', where the foo is
# different for each user or topic or whatever.  We use this to
# remove the 'foo' part, giving the 'contentful' prefix of the key.
_KEY_PREFIXES = ('_ag5',
                 ':ag5',
                 'last_action_cache_2_',
                 'notifications_dict_for_',
                 'VoteRateLimiter',
                 'FlagRateLimiter',
                 'api_topic_videos',
                 'identity_bucket',
                 'bingo_random',
                 'profile_from_userid:',
                 'blog_posts_id=',
                 'oauth_token_1/',
                 '_gae_alternative:',
                 )

_KEY_PREFIX_RE = re.compile(r'(%s).*' % '|'.join(_KEY_PREFIXES))


class WebRequest(object):
    """Holds information about a single webserver request."""
    def __init__(self):
        self.url = None
        self.time_t = None
        # Each memcache line is a tuple (I'd like to store it as an
        # object, but there's too much overhead).  The tuple is
        # (
        # 0: time_t of request,
        # 1: command: get, set, get_multi, etc,
        # 2: memcache key,
        # 3 <for get requests>: did the get request succeed or fail,
        # 3 <for set requests>: sizeof(value),
        # 4 <for set requests>: when a set expires,
        # )
        self.memcache_lines = []

    def add_memcache_line(self,
                          time_t, command, key, success, num_bytes, expires):
        if command.startswith('get'):
            tup = (time_t, command, key, success)
        elif command.startswith('set'):
            tup = (time_t, command, key, num_bytes, expires)
        else:
            tup = (time_t, command, key)
        self.memcache_lines.append(tup)


def _extract_digits(s):
    """Returns the digits from s, throwing away everything else."""
    if s is None:
        return '0'
    return re.sub('[^0-9]', '', s)


def read_logs(*filenames):
    """Read and return the memcache lines from each passed-in filename.

    Arguments:
       *filenames: a list of filenames to read logs from.  If the
          filename ends with '.gz' we will uncompress and read.
          If the filename is '-', read from stdin.

    Returns:
       A tuple (list of WebRequest objects, number-of-non-memcache-requests).
       The list of Request objects holds those web requests that caused
       at least one memcache access.  For all other requests, we don't
       store the request, but we do count it, and return that total count
       as the second argument of the return value.
    """
    requests = []
    num_non_memcache_requests = [0]   # list to work inside the closure below
    num_records = [0]                 # for logging

    def _save_request(request):
        if not request:
            return
        num_records[0] += 1
        if num_records[0] % 1000 == 0:
            print >> sys.stderr, 'Processed %s requests' % num_records[0]
        if request.memcache_lines:
            requests.append(request)
        else:
            num_non_memcache_requests[0] += 1

    for filename in filenames:
        if filename == '-':
            f = sys.stdin
        elif filename.endswith('.gz'):
            f = gzip.open(filename)
        else:
            f = open(filename)

        current_request = None
        for line in f:
            # In the logs, request lines start right away, while logging
            # lines start with a tab.
            if not line.startswith('\t'):
                m = _REQUEST_LINE_RE.match(line)
                if m:
                    _save_request(current_request)
                    current_request = WebRequest()
                    current_request.time_t = time.mktime(time.strptime(
                        m.group('time'), _REQUEST_LINE_DATE_FORMAT))
                    current_request.url = m.group('url')
            elif current_request:
                m = _MEMCACHE_LINE_RE.match(line)
                if m:
                    current_request.add_memcache_line(
                        float(m.group('time_t')),
                        m.group('command'),
                        m.group('key').strip('"'),
                        m.group('success') != '(fail)',
                        int(_extract_digits(m.group('num_bytes'))),
                        int(_extract_digits(m.group('expires')))
                        )
        # Save the last request in the file as well
        _save_request(current_request)

    # Sort the requests by time.
    requests.sort(key=lambda r: r.time_t)
    return (requests, num_non_memcache_requests[0])


def memcache_lines(requests):
    """Give the memcache lines in order, when you don't care about requests."""
    for request in requests:
        for m in request.memcache_lines:
            yield m


def _incr(m, k, delta=1):
    """m[k] += 1, inserting k if it doesn't already exist."""
    m.setdefault(k, 0)
    m[k] += delta


def _sort_by_value(m):
    """Returns an list of (key, value) of m, sorted by decreasing value."""
    r = list(m.items())
    r.sort(key=lambda kv: kv[1], reverse=True)
    return r


def print_value_sorted_map(m, max_to_print=None):
    """Print '(int) value: key' for every item in m, sorted by decr value."""
    if not m:
        print '<none>'
        return

    keyvals = _sort_by_value(m)
    largest_value = keyvals[0][1]
    max_digits = len(str(largest_value))
    num_printed = 0
    for (k, v) in keyvals:
        print '%*d: %s' % (max_digits, v, k)
        num_printed += 1
        if (max_to_print is not None) and num_printed >= max_to_print:
            break


def key_prefix(key):
    """Give a prefix of the key that tries to capture its 'type'."""
    # For instance, foo_agl2344321 and foo_agl5252133 will share a prefix.
    return _KEY_PREFIX_RE.sub(lambda m: m.group(1) + '...', key)


def print_header(title, prologue):
    print
    print '-' * 70
    print title.upper()
    print
    print prologue.strip()
    print


g_analyses_to_run = []


def run(func):
    """Add this decorator to a function and it will be run in main."""
    global g_analyses_to_run
    g_analyses_to_run.append(func)
    return func


# -----------------------------------------------------------------------
# The analyses
# -----------------------------------------------------------------------


@run
def print_distribution(requests):
    """Print the distribution of memcache operations."""
    counts = {}
    num_success = 0   # for get requests
    num_fail = 0
    for memcache_line in memcache_lines(requests):
        key = memcache_line[1]   # command
        _incr(counts, key)

        # Also keep track of how many get's succeeded vs failed
        if key.startswith('get'):
            if memcache_line[3]:   # success
                num_success += 1
            else:
                num_fail += 1

    print_header('DISTRIBUTION OF MEMCACHE REQUEST TYPES',
                 'The types are "get", "set", etc.')
    for k in sorted(counts):
        print '%s: %s' % (k, counts[k])

    success_percent = num_success * 100.0 / (num_success + num_fail)
    print
    print 'successful gets: %s (%.2f%%)' % (num_success, success_percent)
    print 'failed gets: %s (%.2f%%)' % (num_fail, 100.0 - success_percent)


@run
def print_failed_gets(requests):
    """For each key-prefix, print how many times a 'get' on it failed."""
    counts = {}
    for memcache_line in memcache_lines(requests):
        if memcache_line[1].startswith('get') and not memcache_line[3]:
            _incr(counts, key_prefix(memcache_line[2]))

    print_header('FAILED GETS',
                 'The number of times we did a memcache "get" for this\n'
                 'kind of key, and the "get" failed.')
    print_value_sorted_map(counts)


@run
def print_memcache_access_pattern_per_http_request(requests):
    """Print info about number of failed/successful gets per web request."""
    count = {}    # key is (#failed, #set, #successful)
    total_fail = 0
    total_success = 0

    for request in requests:
        num_fail = 0
        num_success = 0
        num_set = 0
        for memcache_line in request.memcache_lines:
            if memcache_line[1].startswith('get'):
                if memcache_line[3]:   # success
                    num_success += 1
                else:
                    num_fail += 1
            elif memcache_line[1].startswith('set'):
                num_set += 1
        _incr(count, (num_fail, num_set, num_success))
        if num_set and num_fail:
            total_fail += 1
        else:
            total_success += 1

    print_header('MEMCACHE ACCESS PATTERN PER HTTP REQUEST',
                 'For each http request *that resulted in at least one\n'
                 'memcache request, we calculated a triple:\n'
                 '  (# of failed gets, # of sets, # of successful gets)\n'
                 'Then we counted how often each triple occurred; that\n'
                 'is the histogram below.  So "4: (1, 2, 3)" means we\n'
                 'saw 4 different HTTP requests that resulted in 1 failed\n'
                 'memcache-get, 2 memcache-sets, and 3 successful\n'
                 'memcache-gets.')
    # Here we want to sort by key, not value, so we do it ourselves.
    for k in sorted(count, reverse=True):
        print '%s: %s' % (k, count[k])
    print
    print 'TOTAL #req with >=1 failed get and >=1 set: %s' % total_fail
    print 'TOTAL #req without a failed get and set:    %s' % total_success


@run
def print_evicted_gets(requests):
    """For each key-prefix, print how many times a get-after-set failed."""
    set_keys = set()
    evicted_key_prefix_count = {}

    for memcache_line in memcache_lines(requests):
        if memcache_line[1].startswith('set'):
            set_keys.add(memcache_line[2])
        elif (memcache_line[1].startswith('get') and  # get request
              not memcache_line[3] and                # that failed
              memcache_line[2] in set_keys):          # and is after a set
            _incr(evicted_key_prefix_count, key_prefix(memcache_line[2]))

    print_header('EVICTED GETS',
                 'The number of times we did a memcache "get" for this\n'
                 'kind of key, and the "get" failed, and we saw the key\n'
                 'get inserted before.  This means that the key was\n'
                 'inserted and then evicted from the cache before the "get"\n'
                 'happened.\n')
    print_value_sorted_map(evicted_key_prefix_count)


@run
def print_most_gets(requests):
    """Print the 10 keys we do the most successful lookups of."""
    successful_get_count = {}

    for memcache_line in memcache_lines(requests):
        if (memcache_line[1].startswith('get') and  # get request
            memcache_line[3]):                      # that succeeded
            _incr(successful_get_count, memcache_line[2])

    print_header('MOST GETS',
                 'The 10 keys with the most successful "gets".\n'
                 'These keys are the ones making best use of the cache.')
    print_value_sorted_map(successful_get_count, 10)


@run
def print_set_but_never_get(requests):
    """Print info about keys we set in the memcache and never look up."""
    set_but_not_get_keys = set()
    set_and_get_keys = set()

    for memcache_line in memcache_lines(requests):
        if memcache_line[1].startswith('set'):
            set_but_not_get_keys.add(memcache_line[2])
        elif memcache_line[1].startswith('get'):
            if memcache_line[2] in set_but_not_get_keys:
                set_but_not_get_keys.discard(memcache_line[2])
                set_and_get_keys.add(memcache_line[2])

    set_no_get_count = {}
    for k in set_but_not_get_keys:
        _incr(set_no_get_count, key_prefix(k))

    set_and_get_count = {}
    for k in set_and_get_keys:
        _incr(set_and_get_count, key_prefix(k))

    retval = []
    for k in set_no_get_count:
        retval.append((k, set_no_get_count[k], set_and_get_count.get(k, 0)))
    retval.sort(key=lambda x: -x[1])

    print_header('SET BUT NEVER GET',
                 'Keys that are set but we never try to get.\n'
                 'They are grouped by key-prefix, so "4: access_token_..."\n'
                 'means that 4 keys that start with "access_token_" were\n'
                 'set but never retrieved: these are "wasted" memcache\n'
                 'items. To understand them better, we also list how many\n'
                 'keys with the given prefix were set *and* looked up\n'
                 'later; comparing the two lets us know if set-but-no-get\n'
                 'is the common or the uncommon case.\n'
                 'Format is <#set-but-not-get> <#set-and-get>: <key prefix>\n')
    for (k, v1, v2) in retval:
        print '%s %s: %s' % (v1, v2, k)


@run
def print_get_but_never_set(requests):
    """Print keys we successfully get but never set."""
    set_keys = set()
    get_but_not_set_count = {}

    for memcache_line in memcache_lines(requests):
        if memcache_line[1].startswith('set'):
            set_keys.add(memcache_line[2])
        elif (memcache_line[1].startswith('get') and
              memcache_line[3] and                # successful get
              memcache_line[2] not in set_keys):  # ...but no previous set
            _incr(get_but_not_set_count, key_prefix(memcache_line[2]))

    print_header('GET WITHOUT PREVIOUS SET',
                 'Keys that had a successful get, but we never saw a set.\n'
                 'These are values that have lived in the memcache for\n'
                 'so long, and accessed so often, that they never need\n'
                 'to be set for the entire length of the log run.')
    print_value_sorted_map(get_but_not_set_count)


@run
def print_space_usage(requests):
    """Print info on which keys take up how much space in the cache."""
    # The problem here is keys being inserted multiple times.
    # We only count the size taken by the last one.
    key_sizes = {}
    for memcache_line in memcache_lines(requests):
        if memcache_line[1].startswith('set'):
            key_sizes[memcache_line[2]] = (len(memcache_line[2]) +
                                           memcache_line[3])   # sizeof(value)

    key_prefix_sizes = {}
    total = 0
    for k in key_sizes:
        _incr(key_prefix_sizes, key_prefix(k), delta=key_sizes[k])
        total += key_sizes[k]

    print_header('WHERE THE MEMORY GOES',
                 'For each key-prefix, how many bytes it takes in the cache.\n'
                 '(We could be over-estimating for any given key, since\n'
                 'we don\'t know what keys have been evicted from the cache.')
    print_value_sorted_map(key_prefix_sizes)
    print
    print 'TOTAL: %s bytes (%.1f M)' % (total, total / 1024.0 / 1024.0)
    print 'TOTAL: %s items' % len(key_sizes)


def main(args):
    (requests, num_non_memcache_requests) = read_logs(*args)

    print_header('ANALYSIS OF HTTP REQUEST LOGS',
                 'Analysis of these files: %s.\n' % ', '.join(args) +
                 'Note that the time-range may be off due to timezone issues.')
    print '# of HTTP requests: %s' % len(requests)
    print 'Time range: %s - %s' % (time.ctime(requests[0].time_t),
                                   time.ctime(requests[-1].time_t))

    print_header('NUMBER OF REQUESTS WITH NO MEMCACHE ACCESS',
                 'Many of these are for static content (.gif, etc).')
    print num_non_memcache_requests

    for fn in g_analyses_to_run:    # set via the @run decorator
        fn(requests)


if __name__ == '__main__':
    main(sys.argv[1:])
