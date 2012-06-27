"""Utility functions to parse streaming tab-delimited input from Hive tables
and group and normalize for custom reducer functions.
"""

# TODO(david): Tests. Really. This is very testable.


import json
import sys


def parse_user_topic_input(callback):
    """Takes input from stdin -- exercise attempts done in topic mode clustered
    on user-topic and sorted by time done (tab-delimited rows from
    topic_attempts table) -- and give that to the given reducer function.

    callback - The reducer function that will actually emit rows. It will be
        given the following arguments:

        attempts - a list of exercise card attempts in a topic, ordered by time
            done. Each element is a tuple (bool correct, int problem_number,
            dict scheduler_info).
        user_topic - tuple of (user, topic ID)
        user_segment - group(s) that the user is a member of for dashboard
            comparison purposes (eg. A/B test experiments, has coach, etc.)
    """

    def should_skip(attempts):
        """Whether the reducer should not be called.

        Skip topics for which we don't have complete information about (those
        for which we don't have info up to the first problem done in that
        topic). We approximate this by ensuring the problem number of the first
        known exercise in this topic is 1.
        """
        # TODO(david): Make this a better heuristic. Actually log card numbers
        #     in stacklogs or something.
        return not attempts or attempts[0][1] != 1

    prev_user_segment = None
    prev_user_topic = None
    attempts = []

    for line in sys.stdin:
        (user, topic, exercise, time_done, time_taken, problem_number, correct,
                scheduler_info, user_segment, dt) = line.rstrip().split('\t')

        user_topic = (user, topic)
        if user_topic != prev_user_topic:
            # We're getting a new user-topic, so perform the reduce operation
            # on our previous group of user-topics
            if not should_skip(attempts):
                callback(attempts, prev_user_topic, prev_user_segment)
            attempts = []

        correct = correct == 'true'
        problem_number = int(problem_number)
        scheduler_info = json.loads(scheduler_info)
        attempts.append((correct, problem_number, scheduler_info))

        prev_user_topic = user_topic
        prev_user_segment = user_segment

    if not should_skip(attempts):
        callback(attempts, prev_user_topic, prev_user_segment)
