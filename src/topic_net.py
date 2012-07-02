#!/usr/bin/env python
"""Fit parameters of a topic level Bayesian network.

For an overview of the motivation, see http://derandomized.com/day/2012/03/27
For details on this implementation, see the docstring for TopicNet.
"""

import json
# TODO(jace): update requirements.txt for scipy?
# TODO(jace): change import style
from numpy import asarray, ones, zeros, mean, sum, arange, log
import numpy as np
import optparse
import scipy.stats
import scipy.optimize.lbfgsb
import sys

import util

g_logger = util.get_logger()

MISSING_VALUE = -1.0


def fit_topics_bnet(in_filename, out_filename):
    """Read the output from bayes_net_dataset.{q|py}, group the data by topic,
    fit a TopicNet for each topic, and output all the results to a file.

    NOTE: Assumes the incoming data of form (topic_name, json), where the
    JSON string is dictionary of the from {exercise_name => accuracy}, and
    that every JSON dictionary for a given topic_name has the same set of
    keys (exercise_names)  See TopicNet documentation for more information
    on the meaning of the data in the JSON string.
    """
    model = TopicNet()

    max_steps = 100
    max_samples = 20000

    topic_data = {}
    topic_models = {}
    line_count = 0

    with open(in_filename) as file:

        for line in file:

            vals = line.split(',', 1)

            if len(vals) != 2:
                g_logger.error("Line must have two comma-separated values: "
                               "[%s]" % line)
                continue

            topic, sample = vals

            data = topic_data.setdefault(topic, [])
            data.append(sample)

            line_count += 1
            if line_count % 10000 == 0:
                g_logger.info("Loaded %d data points." % line_count)

    print "Topics loaded: %s" % topic_data.keys()

    np.seterr(all='warn')

    for topic, data in topic_data.iteritems():

        T, E, col_names = model.parse_data(data[:max_samples])

        print "======================"
        print "START TOPIC = %s" % topic
        print E.shape
        print E
        print

        theta_learned = model.learn(T, E, max_steps, sample_hidden=True)

        print 'Starting State for Topic=%s:' % topic
        model.print_theta(model._maximization(T, E))
        print 'Ending State for Topic=%s:' % topic
        model.print_theta(theta_learned)

        print "Column names:"
        for i, col_name in enumerate(col_names):
            print "E%d : %s" % (i, col_name)

        print "END TOPIC = %s" % topic
        print "======================"

        topic_models[topic] = create_topic_model(col_names, theta_learned)

    with open(out_filename, 'w') as outfile:
        json.dump(topic_models, outfile, indent=2)


class TopicNet:
    """Functions to compute a topic-level Bayesian net.

    The hidden T variable is models as a discrete (binary) variable.
    The E variables are modeled with a beta distribution conditional on T.

    In the code below, T is always as 1d array of data for the T variable.
    E is always a matrix of data, [sample_row x topic_exercise].  The rows
    in the E matrix represent a snapshot in time of some user's "accuracy"
    on each the exercises in the topic.  The accuracy should between 0.0
    and 1.0, or -1.0 if the user has not attempted any problems on that
    exercise.  This class is indifferent to the specific accuracy measure
    chosen, provided it meets those criteria.

    theta is a tuple of the form (theta_T, theta_E) which parameterizes the
    joint distribution.  theta_T is a scalar equal to the probability
    of being equal to 1.  theta_E is 3d array, dimensions are
    [topic_exercise, value_of_T, alpha/beta].  The alpha index is 0, beta is 1.
    """

    def _expectation(self, E, I, theta, sample_hidden):
        """Impute the hidden T variable based on observed E evidence."""
        theta_T, theta_E = theta

        # init unnormalized joint probability of E data if the hidden T unit==0
        p_E_T0 = ones(E.shape[0], dtype=float)
        # init unnormalized joint probability of E data if the hidden T unit==1
        p_E_T1 = ones(E.shape[0], dtype=float)

        for col in range(theta_E.shape[0]):

            # I indicates whether E_col is present (not missing) for each row
            I = E[:, col] != MISSING_VALUE

            alpha, beta = theta_E[col, 0, :]
            densities = scipy.stats.beta.pdf(E[:, col], alpha, beta)
            p_E_T0 *= (I * densities) + (I == 0) * 1.0

            alpha, beta = theta_E[col, 1, :]
            densities = scipy.stats.beta.pdf(E[:, col], alpha, beta)
            p_E_T1 *= (I * densities) + (I == 0) * 1.0

        prob_0_unnorm = p_E_T0 * (1 - theta_T)
        prob_1_unnorm = p_E_T1 * theta_T

        hidden = prob_1_unnorm / (prob_0_unnorm + prob_1_unnorm)

        if sample_hidden:
            # set the hidden unit to a 0 or 1 instead
            # of a probability of activation
            hidden = (hidden > np.random.random(hidden.shape)) * 1

        return hidden

    def _maximization(self, T, E):
        """Compute the params (theta) that maximize the likelihood of data."""
        theta_T = mean(T)  # the probability is just the average activation
        theta_E = ones([E.shape[1], 2, 2], dtype=float)  # initializing

        for e in range(E.shape[1]):
            E_col = E[:, e]

            def objective((alpha, beta), t_state):
                """Return the (negative) loglikelihood of the E column."""
                ix = E_col != MISSING_VALUE
                ix = np.logical_and(ix, T == t_state)

                densities = scipy.stats.beta.pdf(E_col[ix], alpha, beta)

                return -sum(log(densities))

            def optimize(t_state):

                results = scipy.optimize.lbfgsb.fmin_l_bfgs_b(
                    objective,
                    # TODO(jace): pass in previous theta as starting point?
                    tuple(theta_E[e, t_state, :]),  # initial position
                    args=(t_state,),
                    approx_grad=True,
                    bounds=[(0.0, None), (0.0, None)],
                    iprint=1,
                    disp=0,
                    maxfun=100)

                return results[0]  # position of the maximum likelihood

            # compute (alpha, beta) for T=0
            theta_E[e, 0, :] = np.asarray(optimize(0))

            # compute (alpha, beta) for T=1
            theta_E[e, 1, :] = np.asarray(optimize(1))

        return [theta_T, theta_E]

    def _log_likelihood(self, T, E, theta):
        """Copmute the loglikelihood of the data (used for debugging)."""

        theta_T, theta_E = theta

        p_E_T0 = ones(E.shape[0], dtype=float)
        p_E_T1 = ones(E.shape[0], dtype=float)

        for col in range(theta_E.shape[0]):

            I_col = E[:, col] != MISSING_VALUE

            alpha, beta = theta_E[col, 0, :]
            densities = scipy.stats.beta.pdf(E[:, col], alpha, beta)
            p_E_T0 *= (I_col * densities) + (I_col == 0) * 1.0

            alpha, beta = theta_E[col, 1, :]
            densities = scipy.stats.beta.pdf(E[:, col], alpha, beta)
            p_E_T1 *= (I_col * densities) + (I_col == 0) * 1.0

        prob_0_unnorm = p_E_T0 * (1 - theta_T)
        prob_1_unnorm = p_E_T1 * theta_T

        likelihoods = T * prob_1_unnorm + (1 - T) * prob_0_unnorm

        log_likelihood = np.sum(np.log(likelihoods))

        return log_likelihood

    def _flip_theta_if_needed(self, theta):
        """Ensure theta is oriented so that T=1 means more mastery.

        There are two equivalent solutions with T=1 and T=0 flipped.
        I want to choose the one where T=1 means higher mastery on
        the exercises.
        """
        theta_T, theta_E = theta
        # mean of the beta distribution is: alpha / (alpha + beta)
        mean_T0 = theta_E[:, 0, 0] / (theta_E[:, 0, 0] + theta_E[:, 0, 1])
        mean_T1 = theta_E[:, 1, 0] / (theta_E[:, 1, 0] + theta_E[:, 1, 1])
        if np.sum(mean_T0 > mean_T1) > theta_E.shape[0] / 2 + 1:
            print "Flipping the T-orientation of theta."
            theta_T = 1 - theta_T
            temp0 = theta_E[:, 0, :].copy()
            temp1 = theta_E[:, 1, :].copy()
            theta_E[:, 0, :] = temp1
            theta_E[:, 1, :] = temp0

        return (theta_T, theta_E)

    def learn(self, T, E, max_iter, sample_hidden):
        """Use expectation-maximization to find theta for this T and E."""
        # Indicator matrix for whether each E-variable is present
        I = (E != MISSING_VALUE) * 1

        theta = self._maximization(T, E)

        for i in range(max_iter):
            T = self._expectation(E, I, theta, sample_hidden)  # E-step

            theta = self._maximization(T, E)    # M-step

            print "Run %d produced theta of:" % i
            print "Loglikelihood: %f" % self._log_likelihood(T, E, theta)
            self.print_theta(theta)

        return self._flip_theta_if_needed(theta)

    def parse_data(self, data):
        """Parse a list of JSON strings, return a NumPy array.

        Incoming data is of the form (topic_name, json_string), where the
        JSON string represents a dictionary of the form
        {exercise_name => accuracy}, and every JSON dictionary for a
        given topic_name has the same set of keys (exercise_names).

        Return values are of the format:
        T - 1d array of (random) data for the T variable
        E - matrix data for E variables, [sample_row x topic_exercise]
        col_names - the exercise names corresponding to the columns of E
        """
        first_row = json.loads(data[0])

        row_count = len(data)
        col_count = len(first_row)
        col_names = sorted(first_row.keys())

        T = np.random.randint(2, size=row_count)
        E = zeros([row_count, col_count], dtype=float)

        for row_num, row in enumerate(data):

            row = json.loads(row)
            row = sorted(row.items(), key=lambda x: x[0])  # sort by *key*
            row = [item[1] for item in row]  # select only values

            E[row_num, :] = asarray(row)

        return T, E, col_names

    def simulate(self, theta, nsamples):
        """Generates a random data set according to parameters in 'theta'.

        T and E have the same format as in parse_data().
        """
        theta_T, theta_E = theta

        T = (theta_T > np.random.random(nsamples))

        num_cols = theta_E.shape[0]
        E = zeros((nsamples, num_cols), dtype=float)

        for col in range(num_cols):
            # Multiplying by T selects the cases where T=1,
            # Multiplying by 1-T selects the cases where T=0.
            alpha, beta = theta_E[col, 1, 0], theta_E[col, 1, 1]
            E[:, col] += T * np.random.beta(alpha, beta, nsamples)

            alpha, beta = theta_E[col, 0, 1], theta_E[col, 0, 1]
            E[:, col] += (1 - T) * np.random.beta(alpha, beta, nsamples)

        return T, E

    def print_theta(self, theta):
        """Prints theta to stdout."""
        theta_T, theta_E = theta

        print "T\t0: %f\t1:%f" % (1 - theta_T, theta_T)
        for i in range(theta_E.shape[0]):
            def output_tuple(i, alpha, beta):
                return (i, alpha, beta, alpha / (alpha + beta))
            print "E%d T=0\t alpha: %f\t beta:%f \t mean:%f" % (
                    output_tuple(i, theta_E[i, 0, 0], theta_E[i, 0, 1]))
            print "E%d T=1\t alpha: %f\t beta:%f \t mean:%f" % (
                    output_tuple(i, theta_E[i, 1, 0], theta_E[i, 1, 1]))

    def run_simulated_example(self):
        """For testing purposes, follows these steps:
        1) Specify a know joint distrubtion.
        2) Generate a simulated dataset according to that distribution.
        3) Overwrite the T variable with something completely random.
        4) Optionally, delete some of the E data, too.,
        5) See if we can learn back the correct, original distrubtion.
        """

        # start by specifying a TRUE joint distribution, theta.
        theta_T = 0.75  # probability that T is 1
        # 3 E nodes, 2 T states, 2 params (alpha, beta) for E_i distribution
        theta_E = asarray(zeros([3, 2, 2]))
        theta_E[0, 0, 0] = 2.0  # alpha for E0 if T==0
        theta_E[0, 0, 1] = 2.0  # beta for E0 if T==0
        theta_E[0, 1, 0] = 1.0  # alpha for E0 if T==1
        theta_E[0, 1, 1] = 3.0  # beta for E0 if T==1
        theta_E[1, 0, 0] = 3.0  # alpha for E0 if T==0
        theta_E[1, 0, 1] = 3.0  # beta for E0 if T==0
        theta_E[1, 1, 0] = 1.0  # alpha for E0 if T==1
        theta_E[1, 1, 1] = 3.0  # beta for E0 if T==1
        theta_E[2, 0, 0] = 4.0  # alpha for E0 if T==0
        theta_E[2, 0, 1] = 4.0  # beta for E0 if T==0
        theta_E[2, 1, 0] = 5.0  # alpha for E0 if T==1
        theta_E[2, 1, 1] = 5.0  # beta for E0 if T==1

        theta = [theta_T, theta_E]

        # now generate/simulate a dataset according to theta
        row_count = 10000
        print "rowcount = %d" % row_count
        [T, E] = self.simulate(theta, row_count)

        # randomize/hide the 'T' variable, to see if we can re-learn it
        T = np.random.randint(2, size=row_count)

        # in addition, randomly remove between 1 to 2 E-values
        # for each sample as 'missing' data
        for i in range(1):
            null = MISSING_VALUE
            E[arange(row_count), np.random.randint(3, size=row_count)] = null

        print T
        print E

        # finally, try to learn the parameters
        theta_learned = self.learn(T, E, 200, sample_hidden=True)

        print 'Starting State:'
        self.print_theta(self._maximization(T, E))

        print 'Ending State:'
        self.print_theta(theta_learned)

        print 'Goal:'
        self.print_theta(theta)


def create_topic_model(col_names, theta):
    """Create a jsonify-able representation of theta."""
    theta_T, theta_E = theta

    topic_model = {}

    topic_model['T'] = theta_T
    topic_model['E'] = {}
    for i, col_name in enumerate(col_names):
        E_i_0 = {'alpha': theta_E[i, 0, 0], 'beta': theta_E[i, 0, 1]}
        E_i_1 = {'alpha': theta_E[i, 1, 0], 'beta': theta_E[i, 1, 1]}
        topic_model['E'][col_name] = {'0': E_i_0, '1': E_i_1}

    return topic_model


if __name__ == '__main__':

    parser = optparse.OptionParser('%prog <infile> <outfile>')
    options, args = parser.parse_args()
    if not len(args) == 2:
        sys.exit('ERROR: You must provide an input file and an output file.')

    fit_topics_bnet(*args)
