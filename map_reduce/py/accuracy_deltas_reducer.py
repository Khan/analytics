#!/usr/bin/env python

"""A hive reducer script that takes from stdin tab-delimited rows from the
topic_attempts table and emits deltas in accuracy as measured by
consecutive pairs of randomized test cards in topic stacks.
"""

# TODO(david): Tests
# TODO(david): Reduce on time_taken as well.

import sys
import os.path

# Add the directory where table_parser.py is to the Python path.
sys.path.append(os.path.dirname(__file__))

import table_parser


def emit_accuracy_deltas(attempts, user_topic, user_segment):
    """Outputs interpolated deltas in accuracy between every consecutive pair
    of test cards.

    Eg. Ben is doing a topic and his third and sixth cards are randomly chosen
    from that topic for unbiased analytics purposes. Third card was wrong but
    sixth was correct. In total, Ben did 42 cards in this topic as a member of
    the fancy_algorithm card selection algorithm A/B test. We output:

        fancy_algorithm \t 42 \t 3 \t 0.333
        fancy_algorithm \t 42 \t 4 \t 0.333
        fancy_algorithm \t 42 \t 5 \t 0.333

    and in general,

        <segment, # cards done in topic, card #, interpolated accuracy delta>

    The intuition here is that, conditional on Ben having done 42 cards before
    completing the topic or giving up, and knowing only how well he did on the
    unbiased 3rd and 6th test cards, we evenly distribute his improvement (0%
    to 100%) among cards 3, 4, and 5 (each card improved accuracy by 33%).

    attempts - a list of exercise card attempts in a topic, ordered by time
        done. Each element is a tuple (bool correct, int problem_number,
        dict scheduler_info).
    user_topic - tuple of (user, topic ID)
    user_segment - group(s) that the user is a member of for dashboard
        comparison purposes (eg. A/B test experiments, has coach, etc.)
    """

    is_test = lambda info: info.get('purpose', None) == 'randomized'
    test_cards = [(i, x[0]) for i, x in enumerate(attempts, 1) if
                 is_test(x[2])]

    for i in range(1, len(test_cards)):
        prev_card, curr_card = test_cards[i - 1], test_cards[i]
        total_gain = float(curr_card[1]) - float(prev_card[1])
        incremental_gain = total_gain / (curr_card[0] - prev_card[0])
        topic = user_topic[1] if user_topic and len(user_topic) >= 2 else None
        if topic == "any":
            # it is not cool to analyze cards done from various stacks
            # as if they were done with one big, generic stack.  for example,
            # if a user moved from an easy to a difficult topic, you would 
            # likely see accuracy drop on the randomized cards, even though
            # this is very healthy user behavior.
            return
        for i in range(prev_card[0], curr_card[0]):
            # TODO(david): Output and group by user segments (eg.
            #     experiments the user was in).
            print '%s\t%s\t%s\t%s\t%s' % (topic, user_segment, len(attempts),
                    i, incremental_gain)


if __name__ == '__main__':
    table_parser.parse_user_topic_input(emit_accuracy_deltas)
