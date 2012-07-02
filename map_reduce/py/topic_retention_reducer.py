#!/usr/bin/env python

"""A hive reducer script that takes input from the topic_attempts table and
emits retention statistics on topics over the card number and the number and
time taken (TODO(david)).
"""

# TODO(david): Tests


import sys
import os.path

# Add the directory where table_parser.py is to the Python path.
sys.path.append(os.path.dirname(__file__))

import table_parser


def emit_topic_retention(attempts, user_topic, user_segment):
    """Outputs a row for every (topic, segment, card number) to be aggregated
    in Hive.

    More precisely, output row has values
    <topic, user segment, randomized?, "card_number", card #, correct (1 or 0)>

    attempts - a list of exercise card attempts in a topic, ordered by time
        done. Each element is a tuple (bool correct, int problem_number,
        dict scheduler_info).
    user_topic - tuple of (user, topic ID)
    user_segment - group(s) that the user is a member of for dashboard
        comparison purposes (eg. A/B test experiments, has coach, etc.)
    """
    def is_randomized(info):
        # Hive only casts empty strings from custom scripts to false
        return 'TRUE' if info.get('purpose', None) == 'randomized' else ''

    user, topic = user_topic

    # Output retention stats by card number
    # TODO(david): Output time taken buckets
    for i, attempt in enumerate(attempts, 1):
        # Output a row for the given topic and for the psuedo-topic "any",
        # which is an aggregation of all topics
        for pseudo_topic in [topic, 'any']:
            print '%s\t%s\t%s\t%s\t%s\t%s' % (pseudo_topic, user_segment,
                    is_randomized(attempt[2]), "card_number", i,
                    int(attempt[0]))


if __name__ == '__main__':
    table_parser.parse_user_topic_input(emit_topic_retention)
