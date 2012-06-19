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
import optparse
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
                 '__gae_mini_profiler_request_',
                 )

_KEY_PREFIX_RE = re.compile(r'(%s).*' % '|'.join(_KEY_PREFIXES))


def _key_prefix(key):
    """Give a prefix of the key that tries to capture its 'type'."""
    # For instance, foo_agl2344321 and foo_agl5252133 will share a prefix.
    return _KEY_PREFIX_RE.sub(lambda m: m.group(1) + '...', key)


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
        # 3 <for get/getlike requests>: did the get request succeed or fail,
        # 3 <for set/setlike requests>: sizeof(value),
        # 4 <for set/setlike requests>: when a set expires,
        # )
        # TODO(csilvers): instead of using a tuple, use a class + __slots__.
        self.memcache_line_tuples = []

    # incr/decr/offset_multi are here because of what is logged for them,
    # even though they add more like 'replace' and other 'set' commands.
    GETLIKE_COMMANDS = ('get', 'get_multi', 'incr', 'decr', 'offset_multi')

    SETLIKE_COMMANDS = ('set', 'set_multi', 'add', 'add_multi',
                        'replace', 'replace_multi')

    def add_memcache_line(self,
                          time_t, command, key, success, num_bytes, expires):
        if command in self.GETLIKE_COMMANDS:
            tup = (time_t, command, key, success)
        elif command in self.SETLIKE_COMMANDS:
            tup = (time_t, command, key, num_bytes, expires)
        else:
            tup = (time_t, command, key)
        self.memcache_line_tuples.append(tup)

    def memcache_lines(self):
        """All the request's memcache loglines, as ParsedMemcacheLine's."""
        # While it's too expensive, memory-wise, to store all memcache
        # lines in the logs as ParsedMemcacheLine objects (the
        # overhead for an object vs a tuple is very high, and there
        # are hundreds of thousands of these), it's not too expensive
        # to store just the memcache lines for a single request as a
        # ParsedMemcacheLine: there are only a dozen-ish of them, at
        # most.  Having the object makes it much easier to reason
        # about memcache lines in the analysis functions below.
        return [ParsedMemcacheLine(t) for t in self.memcache_line_tuples]

    def has_memcache_lines(self):
        return self.memcache_line_tuples


class ParsedMemcacheLine(object):
    def __init__(self, memcache_line_tuple):
        """memcache_line_tuple is in the format described in WebRequest."""
        self.time_t = memcache_line_tuple[0]
        self.command = memcache_line_tuple[1]
        self.key = memcache_line_tuple[2]

        self.success = None
        self.value_size = None
        self.expires = None
        if self.command in WebRequest.GETLIKE_COMMANDS:
            self.success = memcache_line_tuple[3]
        if self.command in WebRequest.SETLIKE_COMMANDS:
            self.value_size = memcache_line_tuple[3]
            self.expires = memcache_line_tuple[4]
        # For incr/decr/offset commands, appengine stores them as ints.
        if self.command in ('incr', 'decr', 'offset_multi'):
            self.value_size = 4   # size of an int, more or less

    def does_set(self):
        """Whether the command can set a value in the cache."""
        # We count incr/decr/etc here, since they set values, even
        # though WebRequest has them in GETLIKE_COMMANDS.  That is
        # because WebRequest cares about the format in which the
        # memcache line is logged, while we care about the semantics
        # of the command.
        return self.command in ('set', 'set_multi', 'add', 'add_multi',
                                'replace', 'replace_multi',
                                'incr', 'decr', 'offset_multi')

    def does_get(self):
        """Whether the command retrieves a value from the cache."""
        return self.command in ('get', 'get_multi')

    def is_successful_get(self):
        """True if this was a 'get' request and it succeeded."""
        return self.success == True     # None for non-get requests

    def is_failed_get(self):
        """True if this was a 'get' request and it failed."""
        return self.success == False    # None for non-get requests

    def key_prefix(self):
        """Give a prefix of the key that tries to capture its 'type'."""
        # For instance, foo_agl2344321 and foo_agl5252133 will share a prefix.
        return _key_prefix(self.key)


def _extract_digits(s):
    """Returns the digits from s, throwing away everything else."""
    if s is None:
        return '0'
    return re.sub('[^0-9]', '', s)


def read_logs(filenames, ignore_before=None):
    """Read and return the memcache lines from each passed-in filename.

    Arguments:
       filenames: a list of filenames to read logs from.  If the
          filename ends with '.gz' we will uncompress and read.
          If the filename is '-', read from stdin.
       ignore_before: if not None, we ignore all records that are seen
          before (in time) a record that has ignore_before as a substring
          in one of its memcache: or request lines.  This allow us to,
          say, send a special request to appengine just at the point we
          want to start analyzing the logs.  As another example,
          '__layer_cache_setting_model._get_settings_dict__" (fail)'
          is a way to avoid records before a memcache flush, since
          _get_settings_dict__ should always be in the memcache under
          normal circumstances.

    Returns:
       A tuple (list of WebRequest objects, number-of-non-memcache-requests).
       The list of Request objects holds those web requests that caused
       at least one memcache access.  For all other requests, we don't
       store the request, but we do count it, and return that total count
       as the second argument of the return value.  NOTE: number-of-non-
       memcache-requests doesn't interact with ignore_before, so that
       number may be high if ignore_before is set.
    """
    requests = []
    num_non_memcache_requests = [0]   # list to work inside the closure below
    num_records = [0]                 # for logging
    # None means 'haven't figured out when to ignore until.'
    # 0 means 'Ignore until 1969' which means 'don't ignore anything at all'.
    ignore_before_this_time = None if ignore_before else 0

    def _save_request(request):
        if not request:
            return
        num_records[0] += 1
        if num_records[0] % 1000 == 0:
            print >> sys.stderr, 'Processed %s requests' % num_records[0]
        if request.has_memcache_lines():
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
            if (ignore_before_this_time is None and
                ignore_before in line and
                current_request):
                ignore_before_this_time = current_request.time_t
                print >> sys.stderr, 'Found ignore-before line:', line.strip()

        # Save the last request in the file as well
        _save_request(current_request)

    # Sort the requests by time.
    requests.sort(key=lambda r: r.time_t)
    # If ignore_before_this_time is set, ditch all requests whose
    # time_t is < ignore_before_this_time.
    if ignore_before_this_time is None:
        print ('WARNING: Never saw the ignore-before text "%s".  Not ignoring.'
               % ignore_before)
    else:
        old_num_requests = len(requests)
        requests = [r for r in requests if r.time_t >= ignore_before_this_time]
        new_num_requests = len(requests)
        if old_num_requests > new_num_requests:
            print ('NOTE: Ignoring %s requests due to ignore-before text "%s"'
                   ' (seen at time-t %s)'
                   % (old_num_requests - new_num_requests, ignore_before,
                      ignore_before_this_time))

    return (requests, num_non_memcache_requests[0])


# To make subsequent calls to memcache-lines faster.
_g_memcache_line_tuples_cache = {}   # key is the tuple of requests


def memcache_lines(requests):
    """Give the memcache lines in order, when you don't care about requests."""
    if requests in _g_memcache_line_tuples_cache:
        all_memcache_tuples = _g_memcache_line_tuples_cache[requests]
    else:
        all_memcache_tuples = []
        for request in requests:
            all_memcache_tuples.extend(request.memcache_line_tuples)
        all_memcache_tuples.sort(key=lambda tup: tup[0])  # sort by time-t
        _g_memcache_line_tuples_cache[requests] = tuple(all_memcache_tuples)

    for tup in all_memcache_tuples:
        yield ParsedMemcacheLine(tup)


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
    total = 0
    for (k, v) in keyvals:
        if (max_to_print is None) or num_printed < max_to_print:
            print '%*d: %s' % (max_digits, v, k)
            num_printed += 1
        total += v
    print '-' * max_digits
    print '%*d: %s' % (max_digits, total, 'TOTAL')


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
        _incr(counts, memcache_line.command)

        # Also keep track of how many get's succeeded vs failed
        if memcache_line.is_successful_get():
            num_success += 1
        elif memcache_line.is_failed_get():
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
def print_is_failed_gets(requests):
    """For each key-prefix, print how many times a 'get' on it failed."""
    counts = {}
    for memcache_line in memcache_lines(requests):
        if memcache_line.is_failed_get():
            _incr(counts, memcache_line.key_prefix())

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
        for memcache_line in request.memcache_lines():
            if memcache_line.is_successful_get():
                num_success += 1
            elif memcache_line.is_failed_get():
                num_fail += 1
            elif memcache_line.does_set():
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
        if memcache_line.does_set():
            set_keys.add(memcache_line.key)
        elif (memcache_line.is_failed_get() and     # failed get request
              memcache_line.key in set_keys):       # and is after a set
            _incr(evicted_key_prefix_count, memcache_line.key_prefix())

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
    is_successful_get_count = {}

    for memcache_line in memcache_lines(requests):
        if memcache_line.is_successful_get():
            _incr(is_successful_get_count, memcache_line.key)

    print_header('MOST GETS',
                 'The 10 keys with the most successful "gets".\n'
                 'These keys are the ones making best use of the cache.')
    print_value_sorted_map(is_successful_get_count, 10)


@run
def print_set_but_never_get(requests):
    """Print info about keys we set in the memcache and never look up."""
    set_but_not_get_keys = set()
    set_and_get_keys = set()

    for memcache_line in memcache_lines(requests):
        if memcache_line.does_set():
            set_but_not_get_keys.add(memcache_line.key)
        elif memcache_line.does_get():
            if memcache_line.key in set_but_not_get_keys:
                set_but_not_get_keys.discard(memcache_line.key)
                set_and_get_keys.add(memcache_line.key)

    set_no_get_count = {}
    for k in set_but_not_get_keys:
        _incr(set_no_get_count, _key_prefix(k))

    set_and_get_count = {}
    for k in set_and_get_keys:
        _incr(set_and_get_count, _key_prefix(k))

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
    get_but_not_set_times = set()

    for memcache_line in memcache_lines(requests):
        if memcache_line.does_set():
            set_keys.add(memcache_line.key)
        elif (memcache_line.is_successful_get() and
              memcache_line.key not in set_keys):   # ...but no previous set
            _incr(get_but_not_set_count, memcache_line.key_prefix())
            get_but_not_set_times.add(memcache_line.time_t)

    print_header('GET WITHOUT PREVIOUS SET',
                 'Keys that had a successful get, but we never saw a set.\n'
                 'These are values that have lived in the memcache for\n'
                 'so long, and accessed so often, that they never need\n'
                 'to be set for the entire length of the log run.')
    print_value_sorted_map(get_but_not_set_count)
    print
    min_time = min(get_but_not_set_times)
    max_time = max(get_but_not_set_times)
    print ('These happened in the time_t range %s - %s (%.2f sec.)'
           % (min_time, max_time, max_time - min_time))


@run
def print_space_usage(requests):
    """Print info on which keys take up how much space in the cache."""
    # The problem here is keys being inserted multiple times.
    # We only count the size taken by the last one.
    key_sizes = {}    # value here is (size, True-if-present/False-if-evicted)
    for memcache_line in memcache_lines(requests):
        if memcache_line.does_set():
            entry_size = len(memcache_line.key) + memcache_line.value_size
            key_sizes[memcache_line.key] = [entry_size, True]
        elif (memcache_line.is_failed_get() and       # get request tha failed
              memcache_line.key in key_sizes):        # but we'd seen a set
            # This key looks to have been evicted, so set the value-bool.
            key_sizes[memcache_line.key][1] = False

    key_prefix_sizes = {}
    total = 0
    items = 0
    evicted_key_prefix_sizes = {}
    evicted_total = 0
    evicted_items = 0
    for k in key_sizes:
        if key_sizes[k][1]:   # not evicted
            _incr(key_prefix_sizes, _key_prefix(k), delta=key_sizes[k][0])
            total += key_sizes[k][0]
            items += 1
        else:
            _incr(evicted_key_prefix_sizes, _key_prefix(k),
                  delta=key_sizes[k][0])
            evicted_total += key_sizes[k][0]
            evicted_items += 1

    print_header('WHERE THE MEMORY GOES',
                 'For each key-prefix, how many bytes it takes in the cache.\n'
                 '(We could be over-estimating for any given key, since\n'
                 'we don\'t know what keys have been evicted from the cache.')
    print_value_sorted_map(key_prefix_sizes)
    print
    print 'TOTAL: %.1f M' % (total / 1024.0 / 1024.0)
    print 'TOTAL: %s items' % items

    print
    print '----------'
    print 'In addition, the following keys were present in the memcache'
    print 'at some point, but were not at the end of the logs analysis:'
    print 'they had been evicted and never replaced.'
    print 'NOTE: many other keys (counted in the total above) may also'
    print 'have been evicted but we never noticed because nobody tried'
    print 'to look them up after their eviction.'
    print
    print_value_sorted_map(evicted_key_prefix_sizes)
    print
    print 'TOTAL: %.1f M' % (evicted_total / 1024.0 / 1024.0)
    print 'TOTAL: %s items' % evicted_items


def main(logfiles, ignore_before=None):
    (requests, num_non_memcache_requests) = read_logs(logfiles, ignore_before)

    print_header('ANALYSIS OF HTTP REQUEST LOGS',
                 'Analysis of these files: %s.\n' % ', '.join(args) +
                 'Note that the time-range may be off due to timezone issues.')
    print '# of HTTP requests: %s' % len(requests)
    if requests:
        print ('Time range: %s - %s\n    time_t: %.0f - %.0f\n'
               % (time.ctime(requests[0].time_t), 
                  time.ctime(requests[-1].time_t),
                  requests[0].time_t, requests[-1].time_t))

    print_header('NUMBER OF REQUESTS WITH NO MEMCACHE ACCESS',
                 'Many of these are for static content (.gif, etc).')
    print num_non_memcache_requests

    if requests:
        requests = tuple(requests)      # needed so they can be a hash key
        for fn in g_analyses_to_run:    # set via the @run decorator
            fn(requests)


if __name__ == '__main__':
    parser = optparse.OptionParser('%prog [options] <log-file> ...\n'
                                   'If log-file is "-", read from stdin.')
    parser.add_option('--ignore-before', default=None,
                      help=('Ignore all requests before a request whose '
                            'logline or memcache-logging lines contains '
                            'IGNORE_BEFORE as a substring.  For instance: '
                            '--ignore-before=\'__layer_cache_setting_model.'
                            '_get_settings_dict__" (fail)\' '
                            'is a way to avoid records before a memcache '
                            'flush, since _get_settings_dict__ is normally '
                            'always in the memcache.'))
    options, args = parser.parse_args()
    if not args:
        sys.exit('ERROR: You must specify at least one log-file to analyze.')
    main(args, options.ignore_before)
