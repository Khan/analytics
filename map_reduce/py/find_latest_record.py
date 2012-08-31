#!/usr/bin/env python
"""Reducer script to pick the latest records among a set of snapshots.

This expects two columns: a key and json blob.
This looks at a 'backup_timestamp' property in each record, and emits the
record with the latest timestamp for all records that match a given key.
"""

import argparse
import codecs
import json
import sys


# The following is needed for printing out char var > 128
sys.stdout = codecs.getwriter('utf8')(sys.stdout)


def main(key_prop='key'):
    key = None
    timestamp = None
    json_str = None

    for line in sys.stdin:
        json_object = json.loads(line)
        current_key = json_object[key_prop]
        if current_key != key:
            if json_str:
                print "%s\t%s" % (key, json_str)
            key = current_key
            timestamp = None
            json_str = None
        current_timestamp = -1
        if 'backup_timestamp' in json_object:
            current_timestamp = json_object['backup_timestamp']
        if timestamp is None or current_timestamp > timestamp:
            timestamp = current_timestamp
            json_str = line.rstrip()
    print "%s\t%s" % (key, json_str)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
            '--key', dest='key', default='key',
            help="The property name in the JSON to use as the key")
    args = parser.parse_args()
    main(args.key)
