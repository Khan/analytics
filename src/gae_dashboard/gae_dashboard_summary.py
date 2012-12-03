#!/usr/bin/env python

"""Download a summary of the GAE admin UI.

Usage: echo GAE_PASSWORD | gae_dashboard_summary.py GAE_USERNAME

The summary is printed to standard output as a JSON document or an
error is raised.

The Google Appengine SDK must be installed in /usr/local/google_appengine,
which is the location where it is installed on the analytics machine.
"""

import json
import sys

import gae_dashboard_curl
import parsers


def fetch_summary(rpcserver, app_id):
    """Build a summary of the GAE dashboards.

    The summary is composed of primitive Python datatypes.
    """
    summary = {}
    instances_contents = rpcserver.Send('/instances?app_id=%s' % app_id, None)
    instances_parser = parsers.Instances(instances_contents)
    summary['version'] = instances_parser.version()
    summary['instances_summary'] = instances_parser.raw_summary_dict()
    summary['instances'] = instances_parser.raw_detail_dicts()
    return summary


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print >>sys.stderr, (
            'Usage: echo GAE_PASSWORD | gae_dashboard_summary.py GAE_USERNAME')
    _, email = sys.argv
    password = sys.stdin.read().rstrip('\n')
    rpcserver = gae_dashboard_curl.create_rpcserver(email, password)
    summary = fetch_summary(rpcserver, 's~khan-academy')
    json.dump(summary, sys.stdout, sort_keys=True, indent=4)
