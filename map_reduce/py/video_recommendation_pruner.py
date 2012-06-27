#!/usr/bin/env python

"""A custom reducer for pruning a video-to-video co-occurence matrix.

This accepts entries for a video co-occurence matrix, clustered by
the first video of each video pair, and emits the NUM_BEST most
correlated videos for each video with a score for each

Input expected in tab-delimited lines containing matrix entries of the form:

(video_i, video_j, preceed_count, succeed_count, videoi_count, videoj_count)

Where:
preceed_count - the number of times a user watched videoi, then watched videoj
succeed_count - the number of times a user watched videoj, then watched videoi
videoi_count - the number of times videoi was watched
videoj_count - the number of times videoj was watched

Output:
Emits lines of the following format:
(video_i, video_j, score)

Where score represents the similiarity score for use in a suggestion algorithm.
Note that scores may not necessarily be symmetric - this will represent
video_j's score after having watched video_i.
"""


import heapq
import math
import sys


DELIMITER = '\t'
NUM_BEST = 20
_IN = sys.stdin  # For unit testing.
_OUT = sys.stdout


def emit_best_pairs(video_key, scored_pairs):
    """Emit a video and its computed best pairs of videos.
    Arguments:
        video_key - a string for the video key these pairs belong to
        scored_pairs - a list of [vid1, vid2, score] values
    """
    if not video_key:
        return

    best_pairs = heapq.nlargest(NUM_BEST,
                                scored_pairs,
                                key=lambda scored_pair: scored_pair[2])
    for vid1, vid2, score in best_pairs:
        _OUT.write(DELIMITER.join([vid1, vid2, str(score)]) + '\n')


def compute_score(preceed_count, succeed_count, video1_count, video2_count):
    return (preceed_count + succeed_count) / math.sqrt(video2_count)
    

def main():
    last_video = None
    scored_pairs = []

    for line in _IN:
        if not line:
            # EOF
            break

        parts = line.rstrip().split(DELIMITER)

        if len(parts) != 6:
            # TODO(benkomalo): error handling.
            continue

        (vid1_key, vid2_key,
         preceed_count, succeed_count,
         video1_count, video2_count) = parts

        try: 
            preceed_count = int(preceed_count)
            succeed_count = int(succeed_count)
            video1_count = int(video1_count)
            video2_count = int(video2_count)
        except ValueError:
            # Some of the values were invalid - deal with it.
            # TODO(benkomalo): error handling.
            continue

        if last_video != vid1_key:
            emit_best_pairs(last_video, scored_pairs)
            scored_pairs = []

        # TODO(benkomalo): do we have to re-normalize after pruning?
        score = compute_score(preceed_count, succeed_count,
                              video1_count, video2_count)
        scored_pairs.append([vid1_key, vid2_key, score])
        last_video = vid1_key

    emit_best_pairs(last_video, scored_pairs)


if __name__ == '__main__':
    main()

