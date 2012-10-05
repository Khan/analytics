#!/usr/bin/env python

"""Scrape the GAE /instances dashboard into JSON.

Usage: instance_report.py <instances.html
"""

import json
import sys

import parsers

if __name__ == '__main__':
    instances_parser = parsers.Instances(sys.stdin.read())
    json.dump(instances_parser.detail_dicts(),
              sys.stdout,
              sort_keys=True,
              indent=4)
