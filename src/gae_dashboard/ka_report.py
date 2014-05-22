#!/usr/bin/env python

"""Store data from App Engine's admin UI in the analytics database.

Data from /memcache are sent to graphite with the prefix
"webapp.gae.dashboard.memcache":

  utc_datetime: DATETIME
  hit_count: INTEGER
  miss_count: INTEGER
  hit_ratio: FLOAT
  item_count: INTEGER
  total_cache_size_bytes: INTEGER
  oldest_item_age_seconds: INTEGER

Data from /instance_summary are sent to graphite with the prefix
"webapp.gae.dashboard.instances":

  utc_datetime: DATETIME
  num_instances: INTEGER
  average_qps: FLOAT
  average_latency_ms: FLOAT
  average_memory_mb: FLOAT

"""

import argparse
import datetime
import sys

import gae_dashboard_scrape
import graphite_util


def report_instance_summary(summary, download_dt, graphite_host,
                            verbose=False, dry_run=False):
    """Send instance summary to graphite.

    Arguments:
      summary: Dict returned by parsers.InstanceSummary.summary().
      download_dt: Datetime when /instance_summary was downloaded.
      graphite_host: host:port of graphite server to send data to, or ''/None
      verbose: If True, print report to stdout.
      dry_run: If True, do not store report in the database.
    """
    record = {'utc_datetime': download_dt,
              'num_instances': summary['total_instances'],
              'average_qps': summary['average_qps'],
              'average_latency_ms': summary['average_latency_ms'],
              'average_memory_mb': summary['average_memory_mb'],
              }
    if verbose:
        print record

    if not dry_run:
        graphite_util.maybe_send_to_graphite(graphite_host, 'instances',
                                             [record])


def report_memcache_statistics(stats, download_dt, graphite_host,
                               verbose=False, dry_run=False):
    """Store memcache statistics in mongo and maybe graphite.

    Arguments:
      stats: Dict returned by parsers.Memcache.statistics().
      download_dt: Datetime when /memcache was downloaded.
      graphite_host: host:port of graphite server to send data to, or ''/None
      verbose: If True, print report to stdout.
      dry_run: If True, do not store report in the database.
    """
    record = {'utc_datetime': download_dt,
              'hit_count': stats['hit_count'].value(),
              'miss_count': stats['miss_count'].value(),
              'hit_ratio': stats['hit_ratio'].value(),
              'item_count': stats['item_count'].value(),
              'total_cache_size_bytes': stats['total_cache_size'].value(),
              'oldest_item_age_seconds': stats['oldest_item_age'].value(),
              }
    if verbose:
        print record

    if not dry_run:
        graphite_util.maybe_send_to_graphite(graphite_host, 'memcache',
                                             [record])


def main():
    parser = argparse.ArgumentParser(description=__doc__.split('\n\n', 1)[0])
    parser.add_argument('--graphite_host',
                        default='carbon.hostedgraphite.com:2004',
                        help=('host:port to send stats to graphite '
                              '(using the pickle protocol). '
                              '(Default: %(default)s)'))
    parser.add_argument('-v', '--verbose', action='store_true', default=False,
                        help='print report on stdout')
    parser.add_argument('-n', '--dry-run', action='store_true', default=False,
                        help='do not store report in the database')
    parser.add_argument('-e', '--email', metavar='EMAIL', required=True,
                        help='The username to use.')
    parser.add_argument('-A', '--application', metavar='APP_ID', required=True,
                        help='Set the application.')
    parser.add_argument('-M', '--module', metavar='MODULE',
                        help='Set the module.')
    parser.add_argument('-V', '--version', metavar='VERSION',
                        help='Set the (major) version.')
    args = parser.parse_args()
    password = sys.stdin.read().rstrip('\n')

    download_dt = datetime.datetime.utcnow()
    scraped = gae_dashboard_scrape.scrape(args.email,
                                          password,
                                          args.application,
                                          ['instance_summary.summary',
                                           'memcache.statistics',
                                           ],
                                          module=args.module,
                                          version=args.version)

    report_instance_summary(scraped['instance_summary.summary'], download_dt,
                            args.graphite_host, args.verbose, args.dry_run)
    report_memcache_statistics(scraped['memcache.statistics'], download_dt,
                               args.graphite_host, args.verbose, args.dry_run)


if __name__ == '__main__':
    main()
