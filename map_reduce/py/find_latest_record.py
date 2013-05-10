#!/usr/bin/env python
"""Reducer script to pick the latest records among a set of snapshots.

This expects two columns: a key and json blob.
This looks at a 'backup_timestamp' property in each record, and emits the
record with the latest timestamp for all records that match a given key.
"""

import codecs
import json
import optparse
import re
import sys


# The following is needed for printing out char var > 128
sys.stdout = codecs.getwriter('utf8')(sys.stdout)

# We may have trouble parsing some binary data, for reasons I don't fully
# understand.  But https://bugs.launchpad.net/meliae/+bug/876810 gives
# a fix to convert such data to a 'neutral' form.
surrogate = re.compile(r"(?<!\\)\\u([dD][0-9a-fA-F]{3,3})")


def replace_surrogates(sample):
    return surrogate.sub("#S\g<1>", sample)


def main(key_prop='key'):
    key = None
    timestamp = None
    json_str = None

    for line in sys.stdin:

        try:
            json_object = json.loads(line)
        except ValueError:
            # Try one more time, in case binary data is the problem.
            print >>sys.stderr, "Warning: Trouble parsing json '%s'." % line
            json_object = json.loads(replace_surrogates(line))

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
    parser = optparse.OptionParser()
    parser.add_option(
            '--k', '--key', dest='key', default='key',
            help="The property name in the JSON to use as the key")
    options, _ = parser.parse_args()
    main(options.key)
