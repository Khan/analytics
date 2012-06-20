#!/usr/bin/python
"""Reducer script to pick the latest records from UserData updates """
import codecs
import json
import sys
sys.stdout = codecs.getwriter('utf8')(sys.stdout)


def main():
    key = None
    ts = None
    json_str = None
    for line in sys.stdin:
        json_object = json.loads(line)
        current_key = json_object['key']
        if current_key != key: 
            if json_str:
                print "%s\t%s" % (key, json_str)
            key = current_key
            ts = None
            json_str = None
        current_ts = -1
        if 'backup_timestamp' in json_object:
            current_ts = json_object['backup_timestamp']
        if ts is None or current_ts > ts:
            ts = current_ts
            json_str = line.rstrip()
    print "%s\t%s" % (key, json_str)


if __name__ == '__main__':
    main()
