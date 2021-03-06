#!/usr/bin/env python

"""Exports video correlation data in Hive tables to GAE production.
"""

USAGE = """%prog [s3_table_location] [score_type]

Reads lines from files representing the Hive table data for video correlations
as generated by map_reduce/hive/video_recommender.q.

A "score_type" must be specified to tell GAE which scoring algorithm was used.
"""

import datetime
import json
import optparse
import rfc822
import sys
import urllib2

import boto

import boto_util
import oauth_util.consts
import oauth_util.fetch_url as oauth_fetcher


class VideoInfo(object):
    def __init__(self, key, index):
        self.key = key
        self.index = index
        self.best_matches = {}


def get_video_info(video_infos, vid_key):
    if vid_key not in video_infos:
        index = len(video_infos)
        video_infos[vid_key] = VideoInfo(vid_key, index)
    return video_infos[vid_key]


def add_score(video_infos, vid1_key, vid2_key, score):
    vid1_info = get_video_info(video_infos, vid1_key)
    vid2_info = get_video_info(video_infos, vid2_key)
    vid1_info.best_matches[vid2_info.index] = score


def upload_to_gae(score_type, version, video_infos):
    try:
        video_keys = video_infos.keys()
        video_keys.sort(key=lambda video_key: video_infos[video_key].index)

        row_dicts = [None] * len(video_keys)
        for info in video_infos.values():
            row_dicts[info.index] = info.best_matches

        oauth_fetcher.fetch_url(
            '/api/internal/dev/videorec_matrix', {
                'score_type': score_type,
                'version': version,
                'data': json.dumps({
                                    'video_keys': video_keys,
                                    'matrix_rows': row_dicts,
                                    }),
            })
    except urllib2.URLError as e:
        print >> sys.stderr, "Unable to access GAE:"
        print >> sys.stderr, e


def main(table_location, score_type, options):
    boto_util.initialize_creds_from_file()

    # TODO(benkomalo): factor some of this boilerplate out to boto_util
    # Open our input connections.
    s3conn = boto.connect_s3()
    bucket = s3conn.get_bucket('ka-mapreduce')
    path = table_location[len('s3://ka-mapreduce/'):]
    if not path.endswith('/'):
        path = path + '/'
    s3keys = bucket.list(path)

    # Mapping of video key to the info on the other videos which best match
    # that video
    # vid_key -> VideoInfo
    video_infos = {}

    # Note: a table's data may be broken down into multiple files on disk.
    delimiter = '\01'
    lines_read = 0
    version = None  # Use a datestamp as a version.
    for key in s3keys:
        if key.name.endswith('_$.folder$'):
            # S3 meta data - not useful.
            continue

        contents = key.get_contents_as_string()
        version = max(version, key.last_modified)
        for line in contents.split('\n'):

            if not line:
                # EOF
                break

            lines_read += 1
            parts = line.rstrip().split(delimiter)
            if len(parts) != 3:
                # TODO(benkomalo): error handling
                continue

            vid1_key, vid2_key, score = parts

            try:
                score = float(score)
            except ValueError:
                # Some of the values were invalid - deal with it.
                # TODO(benkomalo): error handling.
                continue

            add_score(video_infos, vid1_key, vid2_key, score)

            if lines_read % 1000 == 0:
                print "Read %s lines..." % lines_read

    # Convert version datestamp to a more sane ISO8601 from RFC822
    version = rfc822.parsedate_tz(version)[:6]  # extract YMDHMS from tuple
    version = datetime.datetime(*version).isoformat()

    total_pairs = sum([len(info.best_matches)
                       for info in video_infos.values()])
    print "\nSummary of collected data:"
    print "\tScore type: [%s]" % score_type
    print "\tVersion: [%s]" % version
    print ("\tDetected %d videos, with a total of %d video pair data" %
           (len(video_infos), total_pairs))
    print "Target: %s" % oauth_util.consts.SERVER_URL
    if raw_input("Proceed to upload? [Y/n]: ").lower() in ['', 'y', 'yes']:
        upload_to_gae(score_type, version, video_infos)
        print "Success!"
        print "Run the following to make it live:"
        print "set_video_matrix_version.py '%s' '%s'" % (
                score_type, version)


def parse_command_line_args():
    parser = optparse.OptionParser(USAGE)
    # No actual options yet.

    options, args = parser.parse_args()
    if len(args) < 2:
        print >> sys.stderr, USAGE
        sys.exit(-1)

    return options, args


if __name__ == '__main__':
    options, args = parse_command_line_args()

    table_location = args[0]
    score_type = args[1]

    main(table_location, score_type, options)
