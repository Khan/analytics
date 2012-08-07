#!/usr/bin/env python
"""Reducer script to flatten out the table format by user 
    Input format: (user, other key/val fields) separate by "\t" 
    Ouput format: (user, json) separated by "\t"
    Here is an output example. 
    0.jim.meyer.0	{"math-algebra": ["3", "1", "1606", "1"], "math": ["3", "1", "1606", "1"]}
"""


import json
import optparse
import sys


def get_cmd_line_args():
    parser = optparse.OptionParser(
        usage="%prog [options]",
        description="Reducer script to flatten out topic statistics per user")
    parser.add_option("-k", "--keys", 
        help="fields corresponding to aggregation keys separated by comma")
    parser.add_option("-v", "--values", 
        help="fields corresponding to aggregation values separated by comma")
    # TODO(yunfang): Output a warning with unknown args
    options, _ = parser.parse_args()
    return options


def main():
    options = get_cmd_line_args()
    if not options.keys or not options.values:
        exit(1)
    key_fields = []
    val_fields = []

    for k in options.keys.split(','):
        key_fields.append(int(k))

    for v in options.values.split(','):
        val_fields.append(int(v))
    user = None
    user_dict = {}
    for line in sys.stdin:
        data = line.strip().split('\t')
        current_user = data[0] 
        if current_user != user and user != None: 
            json_string = json.dumps(user_dict)
            print "%s\t%s" % (user, json_string) 
            user_dict = {}
        user = current_user
        keys = []
        values = []
        for k in key_fields:
            keys.append(data[k]) 
        for v in val_fields:
            values.append(data[v]) 
        key = '_'.join(keys).lower().replace(' ', '_')
        user_dict[key] = values

    json_string = json.dumps(user_dict)
    print "%s\t%s" % (user, json_string) 


if __name__ == '__main__':
    main()
