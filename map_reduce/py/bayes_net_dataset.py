#!/usr/bin/env python

"""Reducer script to generate a dataset for Bayes net training.

Input: ProblemLog data clusterd by (user), sorted by (user,time_done).
Output:  (topic_name, json_string) where json_string contains key-value
pairs of exercise-accuracy.

Emits a row for each distinct (user,topic) wherein the user did problems
in at least 2 exercises.  The emitted rows contains the the name of
the topic, followed by a json string that contains key values pairs
for the final learning state (value) for each exercise (key) in the topic.
The learning state is currently chosen to be the ewma_10 from the
accuracy model if some problems have been done, or the special value
of -1.0 if no problems were done on that exercise.  Thus the output
can be grouped by topic to provide a user-by-exercise accuracy
matrix for each topic.
"""

import json
import optparse
import sys

sys.path.append(".")  # TODO(jace) We need a decent packaging scheme
import accuracy_model_baseline as model
import topic_util

# global variables
g_topic_exercise = None
g_exercise_topic = None

NO_EVIDENCE_VAL = -1.0


def complete_exercise_history(problems):
    """Return a boolean indicating if all 1st problems are present in data."""
    exercises_seen = []

    for problem in problems:
        if problem['exercise'] not in exercises_seen:
            # First time seeing this exercise.  Require that prob num is 1.
            if problem['problem_number'] != 1:
                return False
            exercises_seen.append(problem['exercise'])

    return True


def emit_sample(topic, topic_state):
    """Emit a data sample to stdout.

    Given topic_states, which is a dictionary { exercise => AccuracyModel() },
    emit the topic names followed by a JSON string with key-value pairs
    for each exercise in the topic (whether the user did problems in all of
    the exercises or not).
    """

    # default to no evidence for all exercises in this topic
    ex_states = dict([(ex, NO_EVIDENCE_VAL) for ex in g_topic_exercise[topic]])

    # now overwrite for all exercises where problems were done
    for ex, accuracy_model in topic_state.iteritems():
        ex_states[ex] = accuracy_model.exp_moving_avg(0.1)

    print "%s\t%s" % (topic, json.dumps(ex_states))


def emit_data(problems, min_exercises):
    """Given a list of problems for one user, output samples as desired.

    problems - A list of JSON strings, each representing a ProblemLog
    min_exercises - minimum number of distinct exercises done within a topic
    to be included in the dataset sample for that topic
    """

    # If we know we don't have full history for this user, skip her.
    if not problems or not complete_exercise_history(problems):
        return

    # Topic => (Exercise => AccuracyModel)
    topic_states = {}

    # Loop through each problem and collate by topic.
    for problem in problems:

        ex = problem['exercise']

        if ex not in g_exercise_topic:
            continue
        topic = g_exercise_topic[ex]

        topic_state = topic_states.setdefault(topic, {})
        ex_state = topic_state.setdefault(ex, model.AccuracyModel())

        ex_state.update(problem['correct'])

    for topic, topic_state in topic_states.iteritems():
        if len(topic_state) >= min_exercises:
            emit_sample(topic, topic_state)


def get_cmd_line_options():
    parser = optparse.OptionParser()
    # minimum # of exercises done in topic to be included in dataset
    parser.add_option("-m", "--min_exercises", type="int", default=2)
    options, _ = parser.parse_args()
    return options


def main():
    print >>sys.stderr, "Starting main."  # TODO remove

    options = get_cmd_line_options()

    # Load exercise <-> topic mapping from a JSON topic tree
    global g_topic_exercise, g_exercise_topic
    with open("./topic_tree.json") as file:
        topic_tree = json.load(file)
    g_exercise_topic = topic_util.exercise_topic(topic_tree)
    g_topic_exercise = topic_util.topic_exercise(topic_tree)
    print >>sys.stderr, "Loaded topic tree."  # TODO remove

    prev_user = None
    problems = []
    value_errors = 0
    
    for line in sys.stdin:
        user, jsons, dt = line.rstrip('\n').split('\t')

        if not user or not jsons:
            continue
        
        if user != prev_user:
            # We're getting a new user-topic, so perform the reduce operation
            # on our previous group of user-topics
            emit_data(problems, options.min_exercises)
            problems = []

        prev_user = user

        try:
            problem = json.loads(jsons)
            problems.append(problem)
        except ValueError:
            value_errors += 1

    emit_data(problems, options.min_exercises)

    print >>sys.stderr, "Finished main with %d ValueErrors." % value_errors

if __name__ == '__main__':
    main()
