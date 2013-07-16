#!/usr/bin/env python

"""Quick reducer script to parse each kalog and, if it's associated with 
the growth mindset experiment, returns the message text of the header.

Input:
    Tab-delimited rows of [bingo_id, kalog]

Output:
    For each log, emits the value in the x.message_text field, if it exists
"""

import sys

def main():
    key1 = 'x.mindset.message_text'
    key2 = 'id.bingo'
    for line in sys.stdin:
        bingo_id, kalog = line.rstrip('\n').split('\t')
        start = kalog.find(key1)
        end = kalog.find(key2)
        if start >= 0 and end >= 0:
            start += len(key1) + 1
            end -= 1
        message_text = kalog[start:end]
        print '\t'.join([bingo_id, message_text])

if __name__ == '__main__':
    main()
