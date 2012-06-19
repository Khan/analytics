#!/usr/bin/python

import itertools
import sys

"""The reduce step of our first MapReduce, allowing us to find how many users
completed both video i and video j for any i and j,
and to deterimine if that normally happens in one specific order.

Takes from stdin (or other specified input file) input of format:
(Each line is tab-delimited and newline terminated:
    for ease of reading here I use tuple notation instead.
    Lines are in reality formated like "user_i\tvideo_j\ttimestamp_{i,j}\n")
(user_1, video_1, timestamp_{1,1})
(user_1, video_2, timestamp_{1,2})
...
(user_1, video_m, timestamp_{1,m})
...
(user_n, video_m, timestamp_{n,m})

The line '(user_i, video_j, timestamp_{i,j})' is present
iff user_i completed video_j at UNIX time timestamp_{i,j}

Outputs: For every pair of videos watched by a user,
(video_i, video_j, indicator_i, indicator_j)
where
    indicator_i is 1 iff the user watched video_i before video_j,
and indicator_j is 1 iff the user watched video_j before video_i

For more information on this project and a higher-level overview of what's
happening, see:
https://sites.google.com/a/khanacademy.org/forge/technical/data_n/collaborative-filtering-with-emr

"""

_out = sys.stdout  # For testing purposes
_in = sys.stdin  # For testing purposes


def output_tab_delimited(s1, s2, i1, i2):
    """Write given strs and ints to outfile specified above (tab-delimited), 2x
    (the second time, we change order, as necessary for output to be correct.)

    """
    _out.write("%s\t%s\t%d\t%d\n" % (s1, s2, i1, i2))
    _out.write("%s\t%s\t%d\t%d\n" % (s2, s1, i2, i1))


def emit_reducer_output(videos):
    """Given all videos a user watched (list of tuples of (video, timestamp)),
    output the 4-tuples for that user, as defined above.

    """
    for (vid_i, vid_j) in itertools.combinations(videos, 2):
        if float(vid_i[1]) < float(vid_j[1]):
            i_before_j = 1
        else:
            i_before_j = 0
        output_tab_delimited(vid_i[0], vid_j[0], i_before_j, 1 - i_before_j)


def main():
    """Get the input, aggregate all videos and timestamps for each user,
    and pass that to the function that emits it in correct format.

    """
    # Initialize so we can use it later
    last_user = None
    videos = []

    for line in _in:
        line = line.rstrip().split("\t")

        if len(line) != 3:
            sys.stderr.write("Malformed input: '%s'!\n" % "\t".join(line))
            return

        (user, video, timestamp) = line

        if last_user == user:
            videos.append((video, timestamp))
        else:
            emit_reducer_output(videos)  # If len(videos) <= 1, this is no-op
            videos = [(video, timestamp)]
        last_user = user

    emit_reducer_output(videos)  # Make sure we emit for the last user

if __name__ == '__main__':
    main()
