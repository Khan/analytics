#!/usr/bin/env python

"""Reducer script to generate a dataset for accuracy model training."""

import json
import math
import optparse
import random
import sys

import scipy.stats

sys.path.append(".")  # TODO(jace) We need a decent packaging scheme
import accuracy_model_baseline as model


# TODO(jace): move this out once we have a sane source packaging
# and importing scheme.
class FieldIndexer:
    def __init__(self, field_names):
        for i, field in enumerate(field_names):
            self.__dict__[field] = i


# Store column indices of topic_attmpets table for convenience
fields = ['user', 'topic', 'exercise', 'time_done', 'time_taken',
          'problem_number', 'correct', 'scheduler_info', 'user_segment', 'dt']
idx = FieldIndexer(fields)

# Load the parameters for the topic Bayesian networks
topic_models = {}
with open("./topic_net_models.json") as file:
    topic_models = json.load(file)


def compute_T_and_E(topic, exercise, ex_states):
    """Perform inference for T and E according to the Bayes net for this topic.

    T is the probability of topic mastery.
    E, here, is estimated accuracy on *this* exercise given the evidence
    on all on *other* exercises.
    See topic_net.py for more details.
    """
    if topic not in topic_models:
        print >> sys.stderr, "ERROR: Topic %s not found in model file" % topic
        return None

    topic_model = topic_models[topic]

    if exercise not in topic_model['E']:
        print >> sys.stderr, ("WARNING: Exercise %s not in model for topic %s."
                              % (exercise, topic))
        return None  # TODO(jace): warn or count these errors

    def alpha_beta(ex_name, T_state):
        """Helper function to retrieve alpha, beta for given ex_name and T."""
        if ex_name not in topic_model['E']:
            return (None, None)  # TODO(jace): warn or count these errors
        return (topic_model['E'][ex_name][T_state]['alpha'],
                topic_model['E'][ex_name][T_state]['beta'])

    # TODO(jace): do we want to include evidence on the query exercise?
    # I suppose that is an empirical question.
    sibling_exercises = [ex for ex in ex_states if ex != exercise]

    # first compute P(T=1)
    p_1 = topic_model['T']
    for sibling_ex in sibling_exercises:
        alpha, beta = alpha_beta(sibling_ex, '1')
        if alpha is None or beta is None:
            continue
        accuracy = ex_states[sibling_ex].exp_moving_avg(0.1)
        p_1 *= scipy.stats.beta.pdf(accuracy, alpha, beta)

    p_0 = 1 - topic_model['T']
    for sibling_ex in sibling_exercises:
        alpha, beta = alpha_beta(sibling_ex, '0')
        if alpha is None or beta is None:
            continue
        accuracy = ex_states[sibling_ex].exp_moving_avg(0.1)
        p_0 *= scipy.stats.beta.pdf(accuracy, alpha, beta)

    T = p_1 / (p_0 + p_1)

    # now do inference for the query exercise
    def mean(T_state):
        alpha, beta = alpha_beta(exercise, T_state)
        return alpha / (alpha + beta)

    E = T * mean('1') + (1 - T) * mean('0')

    print >> sys.stderr, "T, E: ",
    print >> sys.stderr, [T, E]

    return [T, E]


def get_baseline_features(ex_state):
    """Return a list of feature values from the baseline AccuracyModel."""
    if ex_state.total_done:
        log_num_done = math.log(ex_state.total_done)
        pct_correct = float(ex_state.total_correct()) / ex_state.total_done
    else:
        log_num_done = 0.0  # avoid log(0.)
        pct_correct = model.PROBABILITY_FIRST_PROBLEM_CORRECT

    return [ex_state.exp_moving_avg(0.333),
            ex_state.exp_moving_avg(0.1),
            ex_state.streak(),
            log_num_done,
            math.log(ex_state.total_done - ex_state.total_correct() + 1),
            pct_correct]


def emit_sample(attempt, topic_problem_number, ex_states):
    """Emit a single sample vector based on state prior to this attempt."""
    ex = attempt[idx.exercise]
    ex_state = ex_states[ex]  # the accuracy model form

    outlist = []
    outlist += ["%d" % attempt[idx.correct]]
    outlist += ["%.4f" % ex_state.predict()]
    outlist += [attempt[idx.topic]]
    outlist += [attempt[idx.exercise]]
    outlist += ["%d" % attempt[idx.problem_number]]
    outlist += ["%d" % topic_problem_number]

    # print all the feature values for the existing accuracy model
    for feature in get_baseline_features(ex_state):
        outlist += ["%.6f" % feature]

    # print feature values using the Bayes net and evidence on other exercises
    T_and_E = compute_T_and_E(attempt[idx.topic], ex, ex_states)
    if not T_and_E:
        T_and_E = [0.0, 0.0]
    for feature in T_and_E:
        outlist += ["%.6f" % feature]

    sys.stdout.write("\t".join(outlist) + "\n")


def incomplete_history(attempts):
    exercises_seen = []
    for attempt in attempts:
        if attempt[idx.exercise] not in exercises_seen:
            if attempt[idx.problem_number] != 1:
                return True
            exercises_seen.append(attempt[idx.exercise])
    return False


def emit_samples(attempts, options):
    """Given all attempts for a (user, topic), output samples as desired.

    attempts - a list of lists.  the inner lists represent topic_attempts rows.
    """

    # If we know we don't have full history for this user, skip her.
    if incomplete_history(attempts):
        return

    ex_states = {}

    # Loop through each attempt, already in proper time order.
    for i, attempt in enumerate(attempts):

        ex = attempt[idx.exercise]
        ex_state = ex_states.setdefault(ex, model.AccuracyModel())

        # TODO(jace) : support additional sampling schemes
        # Before we update state, see if we want to sample
        if options.sampling_mode == 'nth':
            freq = options.sampling_freq
            if random.randint(1, freq) == freq:
                emit_sample(attempt, i, ex_states)

        elif attempt[idx.scheduler_info].get('purpose', None) == 'randomized':
            emit_sample(attempt, i, ex_states)

        ex_state.update(attempt[idx.correct])


def get_cmd_line_options():
    parser = optparse.OptionParser()
    # should be one of
    # - randomized : use only the random assessment cards
    # - nth : use 1 in N cards as a sample
    parser.add_option("-s", "--sampling_mode", default="randomized")
    parser.add_option("-f", "--sampling_freq", type=int, default=10)
    options, _ = parser.parse_args()
    return options


def main():

    print >>sys.stderr, "Starting main."  # TODO remove

    options = get_cmd_line_options()

    prev_user_topic = None
    attempts = []

    for line in sys.stdin:
        row = line.strip().split('\t')

        user_topic = (row[idx.user], row[idx.topic])
        if user_topic != prev_user_topic:
            # We're getting a new user-topic, so perform the reduce operation
            # on our previous group of user-topics
            emit_samples(attempts, options)
            attempts = []

        row[idx.correct] = row[idx.correct] == 'true'
        row[idx.problem_number] = int(row[idx.problem_number])
        row[idx.scheduler_info] = json.loads(row[idx.scheduler_info])

        attempts.append(row)

        prev_user_topic = user_topic

    emit_samples(attempts, options)

    print >>sys.stderr, "Finished main."  # TODO remove

if __name__ == '__main__':
    main()
