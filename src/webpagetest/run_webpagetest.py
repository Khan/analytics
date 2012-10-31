#!/usr/bin/env python

"""Run the webpagetest batch command to perf-test many KA pages.

webpagetest (www.webpagetest.org) will download the page from any
number of locations, and then render it in a headless browser,
reporting stats about the rendering time.
"""

import csv
import optparse
import os
import sys
import urllib2

import pymongo

import wpt_batch


# The locations/browsers we want to test *from*.  This list comes
# from http://www.webpagetest.org/getLocations.php.

_BROWSER_LOCATIONS = (
    'Dulles_IE8',
    'Dulles_IE9',
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
    try:
        return open('api_key').read().strip()
    except IOError:
        sys.exit('Need to put the webpagetest API key in %s'
                 % os.path.join(os.getcwd(), 'api_key'))


def RunTests(browser_locations, urls_to_test, connectivity_types,
             num_runs_per_url, test_repeat_view, verbose):
    """Get data as a DOM and return a map from url/etc to dom."""
    wpt_options = wpt_batch.GetOptions([])
    wpt_options.server = 'http://www.webpagetest.org/'
    wpt_options.urlfile = urls_to_test
    wpt_options.outputdir = None    # we will handle output ourselves
    wpt_options.key = _ReadKey()
    wpt_options.fvonly = int(not test_repeat_view)
    wpt_options.runs = num_runs_per_url

    # Map from (browser-location, connectivity_type, url) to result DOM
    id_url_dict = {}    # data structure used to parallelize lookups.
    for browser_location in browser_locations:
        for connectivity_type in connectivity_types:
            wpt_options.location = browser_location
            wpt_options.connectivity = connectivity_type

            print ('---\nGetting results for %s (%s)'
                   % (browser_location, connectivity_type))
            sys.stdout.flush()
            # The values of id_url_dict are just used for human
            # readability.  wpt_batch has the value be a url, but for
            # us a url-info tuple is more useful.
            this_id_url_dict = wpt_batch.StartBatch(wpt_options, verbose)
            for (id, url) in this_id_url_dict.iteritems():
                id_url_dict[id] = (browser_location, connectivity_type, url)

    return wpt_batch.FinishBatch(id_url_dict, wpt_options.server,
                                 wpt_options.outputdir, csv_output=True,
                                 verbose=verbose)


def ConvertToDict(browser_location, connectivity_type, url,
                  test_id, dict_output):
    """Take information about one webpagetest result and stores it in a dict.

    dict_output holds the information that webpagetest gives back in its
    csv output, converted to a dict.  We take that information, add in
    other information we have (browser location, etc), and yet other
    information we fetch from the web (.har file).  We return this dict,
    which is in a format suitable to being passed to mongo_db.
    """
    mongo_dict = {'Browser Location': browser_location,
                  'Connectivity Type': connectivity_type,
                  }
    # The string elements from dict_output.
    for k in ('Browser Version', 'Date', 'Time', 'URL'):
        mongo_dict[k] = dict_output[k]

    # The int elements from dict_output.
    for k in ('Activity Time(ms)',
              'Bytes In (Doc)',
              'Bytes In',
              'Bytes Out (Doc)',
              'Bytes Out',
              'Cached',
              'Connections (Doc)',
              'Connections',
              'DNS Lookups (Doc)',
              'DNS Lookups',
              'DOM Content Ready End',
              'DOM Content Ready Start',
              'Doc Complete Time (ms)',
              'Load Event End',
              'Load Event Start',
              'Load Time (ms)',
              'Requests (Doc)',
              'Requests',
              'Run',
              'Time to Base Page Complete (ms)',
              'Time to DOM Element (ms)',
              'Time to First Byte (ms)',
              'Time to Start Render (ms)',
              'Time to Title',
              'Visually Complete (ms)'):
        mongo_dict[k] = int(dict_output[k] or '0')

    # Donwload the har file.
    har_url = ('http://www.webpagetest.org/export.php?test=%s&run=%s&cached=%s'
               % (test_id, mongo_dict['Run'], mongo_dict['Cached']))
    har_contents = urllib2.urlopen(har_url).read()
    mongo_dict['HAR File'] = har_contents

    return mongo_dict


def SaveToMongo(mongo_dicts):
    """Save a series of dicts holding webpagetest data, to mongo."""
    db = pymongo.Connection('107.21.23.204')
    mongo_collection = db['report']['webpagetest_reports']
    mongo_collection.insert(mongo_dicts, safe=True)


def main(args=sys.argv[1:]):
    parser = optparse.OptionParser()
    parser.add_option('-t', '--test', action='store_true', default=False,
                      help='If true, only fetch two urls (to save quota)')
    parser.add_option('-v', '--verbose', action='store_true', default=False,
                      help='If true, print status as we go along')
    options, commandline_args = parser.parse_args(args)
    if commandline_args:
        raise KeyError('No commandline arguments expected, only flags')

    if options.test:
        browser_locations = _BROWSER_LOCATIONS[:2]
        urls_to_test = _URLS_TO_TEST[:1]
        connectivity_types = _CONNECTIVITY_TYPES[:1]
        num_runs_per_url = 1
        test_repeat_view = False
    else:
        browser_locations = _BROWSER_LOCATIONS
        urls_to_test = _URLS_TO_TEST
        connectivity_types = _CONNECTIVITY_TYPES
        num_runs_per_url = _NUM_RUNS_PER_URL
        test_repeat_view = _TEST_REPEAT_VIEW

    _VerifyNumberOfTestsDoesNotExceedThreshold()

    results = RunTests(browser_locations, urls_to_test, connectivity_types,
                       num_runs_per_url, test_repeat_view, options.verbose)

    mongo_dicts = []
    for ((browser_location, connectivity_type, url), 
         (test_id, csv_output)) in results.iteritems():
        reader = csv.DictReader(csv_output)
        for dict_output in reader:
            mongo_dict = ConvertToDict(browser_location, connectivity_type,
                                       url, test_id, dict_output)
            mongo_dicts.append(mongo_dict)

    SaveToMongo(mongo_dicts)
    print 'DONE.  Saved %s dicts to mongo' % len(mongo_dicts)


if __name__ == '__main__':
    main()
