#!/usr/bin/env python

"""Screen-scrape data from the AppEngine admin UI.

This script is generally invoked by setting an email address and
application ID (as set in app.yaml) on the command line and sending a
password on standard input, e.g.,

  echo PASSWORD | gae_dashboard_scrape.py --email=EMAIL -A APP_ID

Available data are printed to standard output as a JSON object or an
error error is raised.  See `gae_dashboard_scrape.py --help` for
available options.


EXAMPLES

Name of the current default version on the default module:

  $ gae_dashboard_scrape.py --module=default memcache.statistics
  memcache.statistics.hit_count	10
  memcache.statistics.miss_count	1
  ...

"""

import argparse
import json
import logging
import string
import sys

import gae_dashboard_curl
import parsers

# This table defines data exposed by the scrape interface. It encodes
# what data to fetch and how to parse it. See _name_parser().
_SCRAPE_TABLE = [
    'deployment.default_version',
    'instance_summary.summaries',
    'instance_summary.summary',
    'memcache.statistics',
    ]


def _name_parser(name):
    """(URL, parser_class, method) needed to scrape data for a name.

    By convention, the first component of the scrape table name is the
    URL to fetch, and, with a little munging, the name of the parser
    class. The second component is a method name on the parser, e.g.,

      'instance_summary.summary'

    is expanded to

      ('/instance_summary', parsers.InstanceSummary, 'summary')

    """
    parser_name, method = name.split('.')
    parser_class_name = string.capwords(parser_name, sep='_').replace('_', '')
    return ('/%s' % parser_name, getattr(parsers, parser_class_name), method)


def scrape(email, password, appid, names, module=None, version=None):
    """Scrape data for each name in names.

    Arguments:
      email: App Engine login email address.
      password: Password matching email.
      appid: An application ID as set in app.yaml.
      names: A list of named data to scrape. See _SCRAPE_TABLE.
      module: (Optional). When scraping, reference this module in the
        version_id query parameter.
      version: (Optional). When scraping, reference this version in the
        version_id query parameter.

    Returns:
      A dict whose keys are the passed-in names and whose values are
    the corresponding return values from the parsers in the parsers
    module.

    """
    unknown_names = set(names) - set(_SCRAPE_TABLE)
    if unknown_names:
        raise ValueError('Unknown names: %s' % sorted(unknown_names))

    data = {}
    dashclient = gae_dashboard_curl.DashboardClient(email, password)

    # Pages may contain multiple pieces of data. Fetch each page once.
    cache = {}
    for name in names:
        res, parser_class, method_name = _name_parser(name)
        parser_key = (res, parser_class)
        if not parser_key in cache:
            url = _build_dashboard_url(res, appid, module=module,
                                       version=version)
            logging.info('Fetching %s' % url)
            cache[parser_key] = parser_class(dashclient.fetch(url))
        logging.info('Reading %s' % name)
        data[name] = getattr(cache[parser_key], method_name)()
    return data


def _build_dashboard_url(url, appid, module=None, version=None):
    """Add app_id (and maybe version_id) query params to App Engine url."""
    url = '%s?app_id=%s' % (url, appid)
    if module and version:
        url += '&version_id=%s:%s' % (module, version)
    elif module:
        url += '&version_id=%s:' % module  # note the trailing ':'
    elif version:
        url += '&version_id=%s' % version
    return url


def _flatten(obj, sep='.'):
    """Flatten a recursive structure of dicts and lists.

    {"cast": [{"mouse": "Jerry"}, {"cat": "Tom"}]}

    becomes

    {"cast.0.mouse": "Jerry", "cast.1.cat": "Tom"}

    """
    flattened = {}

    def enter(obj, name):
        if isinstance(obj, dict):
            for k in obj.keys():
                enter(obj[k], name + [k])
        elif isinstance(obj, list):
            for i, v in enumerate(obj):
                enter(v, name + [str(i)])
        else:
            flatname = sep.join(name)
            if flatname in flattened:
                raise KeyError('Duplicate key %s. Old value=%r, new value=%r'
                               % (flatname, flattened[flatname], obj))
            flattened[flatname] = obj
    enter(obj, [])
    return flattened


def _write_text_format(obj, raw_values=False, stream=sys.stdout):
    """Write obj to stream in lexically ordered, flattened, dotted notation.

    {"cast": [{"mouse": "Jerry"}, {"cat": "Tom"}]}

    becomes

    cast.0.mouse	Jerry
    cast.1.cat	Tom

    Arguments:
      obj: Print this object.
      raw_values: If set, display strings scraped from the UI rather
        than the parsed value, e.g., "100 instances" rather than the
        number 100.
      stream: Print to this stream, default is stdout.

    """
    def write(val, name):
        if isinstance(val, parsers.Value):
            write(val.text() if raw_values else val.value(), name)
        elif isinstance(val, (int, long, float, basestring)):
            print >>stream, '%s\t%s' % (name, val)
        else:
            raise ValueError('No rule to format %r' % val)
    flattened = _flatten(obj)
    for k in sorted(flattened.keys()):
        write(flattened[k], k)


class ValueJSONEncoder(json.JSONEncoder):
    # Extend JSONEncoder to handle Value objects.
    def __init__(self, raw_values=False, *args, **kwargs):
        """Support raw_values argument (see json.JSONEncoder for other args).

        Arguments:
          raw_values: If set, display strings scraped from the UI rather
            than the parsed value, e.g., "100 instances" rather than the
            number 100.

        """
        self.raw_values = raw_values
        super(ValueJSONEncoder, self).__init__(*args, **kwargs)

    def default(self, o):
        if isinstance(o, parsers.Value):
            if self.raw_values:
                return o.text()
            else:
                return o.value()
        # Let the base class default method raise the TypeError.
        return json.JSONEncoder.default(self, o)


def main():
    description, epilog = __doc__.split('\n\n', 1)
    parser = argparse.ArgumentParser(
        description=description,
        epilog=(epilog + '\n\nValid NAMEs include:\n\n  '
                + '\n  '.join(_SCRAPE_TABLE)),
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-v', '--verbose', action='store_true', default=False,
                        help='Print info level logs.')
    parser.add_argument('-e', '--email', metavar='EMAIL', required=True,
                        help='The username to use.')
    parser.add_argument('-A', '--application', metavar='APP_ID', required=True,
                        help='Set the application.')
    parser.add_argument('-M', '--module', metavar='MODULE',
                        help='Set the module.')
    parser.add_argument('-V', '--version', metavar='VERSION',
                        help='Set the (major) version.')
    parser.add_argument('--format', choices=['json', 'text'], default='text',
                        help='Set the output format.')
    parser.add_argument('--raw_values', action='store_true', default=False,
                        help=('Display values as raw field text rather than '
                              'parsed machine-readable numbers.'))
    parser.add_argument('names', nargs='*', metavar='NAME',
                        default=_SCRAPE_TABLE,
                        help='Data to scrape. If not set we scrape it all.')
    args = parser.parse_args()

    root_logger = logging.getLogger()
    if args.verbose:
        root_logger.setLevel(logging.INFO)
    else:
        root_logger.setLevel(logging.CRITICAL)

    password = sys.stdin.read().rstrip('\n')
    scraped = scrape(args.email,
                     password,
                     args.application,
                     args.names,
                     module=args.module,
                     version=args.version)
    if args.format == 'text':
        _write_text_format(scraped, raw_values=args.raw_values)
    elif args.format == 'json':
        print ValueJSONEncoder(raw_values=args.raw_values,
                               indent=4,
                               sort_keys=True).encode(scraped)


if __name__ == '__main__':
    main()
