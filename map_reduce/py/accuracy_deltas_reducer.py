#!/usr/bin/env python

"""A hive reducer script that takes from stdin tab-delimited rows from the
topic_attempts table and emits deltas in accuracy as measured by
consecutive pairs of randomized test cards in topic stacks.
"""

# TODO(david): Tests
# TODO(david): Reduce on time_taken as well.


import json
import sys


def emit_accuracy_deltas(attempts, user_segment):
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
    user_segment - group(s) that the user is a member of for dashboard
        comparison purposes (eg. A/B test experiments, has coach, etc.)
    """

    # Skip topics for which we don't have complete information about (those for
    # which we don't have info up to the first problem done in that topic). We
    # approximate this by ensuring the problem number of the first known
    # exercise in this topic is 1.
    # TODO(david): Make this a better heuristic. Actually log card numbers in
    #     stacklogs or something.
    if not attempts or attempts[0][1] != 1:
        return

    is_test = lambda info: info.get('purpose', None) == 'randomized'
    test_cards = [(i, x[0]) for i, x in enumerate(attempts, 1) if
                 is_test(x[2])]

    for i in range(1, len(test_cards)):
        prev_card, curr_card = test_cards[i - 1], test_cards[i]
        total_gain = float(curr_card[1]) - float(prev_card[1])
        incremental_gain = total_gain / (curr_card[0] - prev_card[0])
        for i in range(prev_card[0], curr_card[0]):
            # TODO(david): Output and group by user segments (eg. experiments
            #     the user was in).
            print '%s\t%s\t%s\t%s' % (user_segment, len(attempts), i,
                    incremental_gain)


def parse_user_topic_input():
    """Takes input from stdin -- exercise attempts done in topic mode clustered
    on user-topic and sorted by time done (tab-delimited rows from
    topic_attempts table) -- and give that to the reducer.
    """

    prev_user_segment = None
    prev_user_topic = None
    attempts = []

    for line in sys.stdin:
        (user, topic, time_done, time_taken, problem_number, correct,
                scheduler_info, user_segment, dt) = line.strip().split('\t')

        user_topic = (user, topic)
        if user_topic != prev_user_topic:
            # We're getting a new user-topic, so perform the reduce operation
            # on our previous group of user-topics
            emit_accuracy_deltas(attempts, prev_user_segment)
            attempts = []

        correct = correct == 'true'
        problem_number = int(problem_number)
        scheduler_info = json.loads(scheduler_info)
        attempts.append((correct, problem_number, scheduler_info))

        prev_user_topic = user_topic
        prev_user_segment = user_segment

    emit_accuracy_deltas(attempts, prev_user_segment)


if __name__ == '__main__':
    parse_user_topic_input()
