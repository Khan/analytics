#!/usr/bin/env python

"""Quick reducer script to parse each kalog and, if it's associated with 
the growth mindset experiment, returns bingo_id and the message 
text of the header.

Input:
    Tab-delimited rows of [bingo_id, kalog]

Output:
    For logs with an x.message_text field, emit the bingo_id and the message
"""

import sys


def main():
    for line in sys.stdin:
        bingo_id, kalog = line.rstrip('\n').split('\t')
        start = kalog.find('x.mindset.message_text')
        if start >= 0:
            start += len('x.mindset.message_text') + 1
            end = kalog.find(';', start)
            message_text = kalog[start:end]
            print '\t'.join([bingo_id, message_text])

if __name__ == '__main__':
    main()
