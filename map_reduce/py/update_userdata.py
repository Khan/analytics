#!/usr/bin/env python
"""Reducer script to pick the latest records from UserData updates """
import codecs
import json
import sys
sys.stdout = codecs.getwriter('utf8')(sys.stdout)


def main():
    key = None
    timestamp = None
    json_str = None
    for line in sys.stdin:
        json_object = json.loads(line)
        current_key = json_object['key']
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
    main()
