#!/usr/bin/env python

"""Run the webpagetest batch command to perf-test many KA pages.

webpagetest (www.webpagetest.org) will download the page from any
number of locations, and then render it in a headless browser,
reporting stats about the rendering time.
"""

import wpt_batch


# The locations/browsers we want to test *from*.  This list comes
# from http://www.webpagetest.org/getLocations.php.

_BROWSER_LOCATIONS = (
    'Dulles_IE8',
    'Dulles_IE10',
    'Dulles:Chrome',
    'Dulles:Firefox',
    'SanJose_IE9',
    'London_IE8',
)

_URLS_TO_TEST = (
    'http://www.khanacademy.org',
    'http://www.khanacademy.org/exercisedashboard',
    'http://www.khanacademy.org/cs/',
    # An arbitrarily picked video
    ('http://khanacademy.org/math/algebra/solving-linear-equations'
     '/v/simple-equations'),
    # An arbitrarily picked exercise
    'http://www.khanacademy.org/math/calculus/e/derivative_intuition',
)

# Options here are DSL, Fios, Dial, and custom.
_CONNECTIVITY_TYPES = (
    'DSL',
)

_NUM_RUNS_PER_URL = 2

# If true, we load each page twice, the second time making use of the
# browser cache, cookies, etc.  If false, we only load the page once,
# assuming empty caches.
_TEST_REPEAT_VIEW = True


def _VerifyNumberOfTestsDoesNotExceedThreshold():
    """Our API key allows 200 requests.  Make sure we're not over that."""
    max_requests = 200
    num_views_per_url = 1 + int(_TEST_REPEAT_VIEW)
    num_requests = (len(_BROWSER_LOCATIONS) * len(_URLS_TO_TEST) *
                    len(_CONNECTIVITY_TYPES) *
                    _NUM_RUNS_PER_URL * num_views_per_url)
    if num_requests > max_requests:
        raise RuntimeError('Number of requests (%s) exceeds allowed quota (%s)'
                           ' -- modify this script to reduce the number.'
                           % (num_requests, max_requests))


def _ReadKey():
    """Read the API key from the local filesystem, and return it."""
    return open('api_key').read().strip()


def RunTests():
    """Get data as a DOM and return a map from url/etc to dom."""
    wpt_options = wpt_batch.GetOptions([])
    wpt_options.server = 'http://www.webpagetest.org/'
    wpt_options.urlfile = _URLS_TO_TEST
    wpt_options.outputdir = None    # we will handle output ourselves
    wpt_options.key = _ReadKey()
    wpt_options.fvonly = int(not _TEST_REPEAT_VIEW)
    wpt_options.runs = _NUM_RUNS_PER_URL

    # Map from (browser-location, connectivity_type, url) to result DOM
    retval = {}
    for browser_location in _BROWSER_LOCATIONS:
        for connectivity_type in _CONNECTIVITY_TYPES:
            wpt_options.location = browser_location
            wpt_options.connectivity = connectivity_type

            print ('---\nGetting results for %s (%s)'
                   % (browser_location, connectivity_type))
            results = wpt_batch.RunBatch(wpt_options, verbose=True)
            for (url, dom) in results.iteritems():
                retval[(browser_location, connectivity_type, url)] = dom

    return retval


def main():
    _VerifyNumberOfTestsDoesNotExceedThreshold()
    results = RunTests()
    for k, v in results.iteritems():
        print '%s\n%s\n' % (k, v.toxml('utf-8'))


if __name__ == '__main__':
    main()
