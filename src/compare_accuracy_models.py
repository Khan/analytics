#!/usr/bin/env python
"""
This script takes in a list of model defintion files (which are python dicts
with "thetas" and "components" entries that have been pickled), and a file
which contains test data.

The script then simulates each model's performance across all of the test data,
calculates ROC curves for each model, for each exercise, and plots all of
these curves to a PDF file.

You need to have matplotlib installed for this to work.
"""

import affinity
import argparse
from collections import defaultdict
import csv
import itertools
import os.path
import pickle

import numpy as np

# necessary to do this after importing numpy to take avantage of
# multiple cores on unix
affinity.set_process_affinity_mask(0, 2 ** multiprocessing.cpu_count() - 1)

import matplotlib

# Improve cross-system compatability for writing PDFs with this call.
# Note, it must be called before pyplot and backend_pdf are imported.
matplotlib.use('Agg')

import matplotlib.pyplot as plt
import matplotlib.backends.backend_pdf as pdf

import accuracy_model_util

# Must be in PYTHONPATH
import knowledge_state
import accuracy_model


class FakeUserExercise(object):
    """Used to pass to knowledge_state.KnowlegeModel.predict()."""

    def __init__(self, exercise):
        self.exercise = exercise
        self._accuracy_model = accuracy_model.AccuracyModel()
        self.problem_number = 0

    def accuracy_model(self):
        return self._accuracy_model


def parse_command_line():
    """Parses the command line and un-pickles the knowledge parameter files.

    Returns: a tuple (options, knowlege_params), where knowlege_params is a
    dict that gets used by downstream processes to keep track of all state
    while simulating the knowledge model. The dict will eventually wind up
    looking something like this:

    {
        model_file1: {
            components: {
                exercise1: [random components],
                exercise2: [random components],
            },

            thetas: [],

            knowledge_states: {
                user1: KnowledgeState,
                user2: KnowledgeState,
            },

            roc_curves: {
                exercise1: {
                    # Keeps track of the total samples analyzed for this ex
                    total_samples: 231,

                    # Tracks the counts of true positives per threshold
                    true_positive: [0,0.01,0.02,...,0.99,1.00],

                    # Tracks the counts of false positives per threshold
                    false_positive: [0,0.01,0.02,...,0.99,1.00],
                },
                exercise2: {
                    # ...
                }
            }
        },
        model_file2: {
            # ...
        }
    }

    """
    parser = argparse.ArgumentParser(description='Compare knowledge models.')

    parser.add_argument("--knowlege_param", action='append',
        help="Path to the knowledge parameter pickle file. Specify one "
             "for each set of parameters you wish to compare.")

    parser.add_argument("--samples",
        help="Path to the CSV of problem log samples. CSV should be formatted "
             "as described by FieldIndexer.plog_fields.")

    parser.add_argument("--graph_pdf", default="graphs.pdf",
        help="Path to write the resulting PDF.")

    parser.add_argument("--num_samples", type=int,
        help="The maximum number of samples to read from the samples file.")

    parser.add_argument("--min_samples", type=int, default=1,
        help="The minimum number of samples to draw graphs for. Any exercise "
             "with few than this many samples will not be plotted.")

    options = parser.parse_args()

    # Translate the file paths into actual knowledge parameters
    knowlege_params = {}
    for kp_path in options.knowlege_param:
        with open(kp_path, "r") as kp_file:
            knowlege_params[kp_path] = pickle.load(kp_file)

    # Investigate the number of shared random components and theta's
    print_parameter_similarities(knowlege_params)

    return options, knowlege_params


def print_parameter_similarities(knowlege_params):
    """Compares each pair of knowledge parameter components and thetas.

    For each pair of knowledge parameters, calculates the number of shared
    random components and thetas, based on keys. New parameters should have
    a superset of old parameter random components. New parameter should share
    a high percentage of old parameter theta values.
    """
    for kpf1, kpf2 in itertools.combinations(knowlege_params.iteritems(), 2):
        kp1 = kpf1[1]
        kp2 = kpf2[1]

        kp1_comp_keys = set(kp1['components'].keys())
        kp2_comp_keys = set(kp2['components'].keys())

        shared_comp_keys = kp1_comp_keys & kp2_comp_keys

        num_shared_values = 0
        for sk in shared_comp_keys:
            # Apparently these are stored as a list of lists, with each
            # sub-list having a value, like:
            # [[-0.04129569], [-0.00283233], [ 0.06143383], ..., [ 0.05374884]]
            # TODO(mattfaus): Validate against NUM_RANDOM_FEATURES
            if all(x == y for x, y in zip(
                    kp1['components'][sk], kp2['components'][sk])):
                num_shared_values += 1

        num_kp1_comp_keys = len(kp1['components'])
        num_kp2_comp_keys = len(kp2['components'])

        print "Random Components %s: %d keys (%d / %.2f%% shared)" % (
            kpf1[0], num_kp1_comp_keys, len(shared_comp_keys),
            (len(shared_comp_keys) / float(num_kp1_comp_keys) * 100))

        print "Random Components %s: %d keys (%d / %.2f%% shared)" % (
            kpf2[0], num_kp2_comp_keys, len(shared_comp_keys),
            (len(shared_comp_keys) / float(num_kp2_comp_keys) * 100))

        print "Shared keys with the same value: %d (%.2f%%)" % (
            len(shared_comp_keys), (
                float(num_shared_values) / len(shared_comp_keys)) * 100)

        kp1_theta_keys = set(kp1['thetas'].keys())
        kp2_theta_keys = set(kp2['thetas'].keys())

        shared_theta_keys = kp1_theta_keys & kp2_theta_keys

        print "Thetas %s: %d keys (%d / %.2f%% shared)" % (kpf1[0],
            len(kp1_theta_keys), len(shared_theta_keys),
            (len(shared_theta_keys) / float(len(kp1_theta_keys)) * 100))

        print "Thetas %s: %d keys (%d / %.2f%% shared)" % (kpf2[0],
            len(kp2_theta_keys), len(shared_theta_keys),
            (len(shared_theta_keys) / float(len(kp2_theta_keys)) * 100))


def iter_samples(options):
    """Yields up to options.num_samples samples from the sample file."""
    with open(options.samples, "r") as sample_file:
        # Format is is as outputed by accuracy_model_featureset.py
        sample_reader = csv.reader(sample_file)

        count = 0
        for sample in sample_reader:
            count += 1

            if count % 10000 == 0:
                print "Processed %d samples" % count

            if options.num_samples and count > options.num_samples:
                break

            yield sample


def main():
    """Simulates knowledge parameters against some problem log samples, keeping
    track of the discrepency between the predicted value and the actual value
    to generate ROC curves and a few other metrics to compare parameter
    performance.

    We must keep state of the following items as we run the simulation.
        1. A UserExercise entity per (knowlege_parameter, user, exercise)
        2. A UserKnowledge entity per (knowlege_parameter, user)

    The samples file should be sorted by user, so we can delete old state once
    we move on to a new user.
    """

    options, knowlege_params = parse_command_line()

    idx = accuracy_model_util.FieldIndexer(
        accuracy_model_util.FieldIndexer.plog_fields)

    # Thresholds used for calculating ROC curves
    thresholds = np.arange(-0.01, 1.02, 0.01)

    # Keep track of UserExercise objects
    user_exercises = {}

    # Keep track of all the exercises we've seen
    seen_exercises = defaultdict(int)

    skipped_samples = 0
    total_samples = 0
    prev_user = None

    for sample in iter_samples(options):
        total_samples += 1
        # Samples are assumed to be in the CSV format created by step 2b in
        # accuracy_model_featureset.py
        user = sample[idx.user]
        exercise = sample[idx.exercise]
        problem_number = int(sample[idx.problem_number])
        problem_type = sample[idx.problem_type]
        label = sample[idx.correct] == 'true'

        # For each super-model
        for kp_file, model_data in knowlege_params.iteritems():

            # Find this user's exercise's state, create one if necessary
            # It is important that these states be stored independently for
            # each model
            # TODO(mattfaus): Merge all this stuff into the
            # model_data['knowledge_states'] data structure
            if (kp_file, user, exercise) not in user_exercises:
                user_exercises[(kp_file, user, exercise)] = (
                    FakeUserExercise(exercise))
            cur_ue = user_exercises[(kp_file, user, exercise)]

            if cur_ue.problem_number + 1 != problem_number:
                # print "Skipping sample due to ordering problem"
                # We will increment this count for every model...
                skipped_samples += 1.0 / (len(knowlege_params))
                continue
            else:
                seen_exercises[exercise] += 1.0 / (len(knowlege_params))
                cur_ue.problem_number += 1

            if 'knowledge_states' not in model_data:
                model_data['knowledge_states'] = {}

            # Find this user's KnowledgeState, if it doesn't exist, create one
            # Also, create a fake User
            if user not in model_data['knowledge_states']:
                model_data['knowledge_states'][user] = (
                    knowledge_state.KnowledgeState())
            cur_ks = model_data['knowledge_states'][user]

            # Set the knowledge params so it doesn't try to read them from disk
            knowledge_state._PARAMS = model_data

            # Predict this sample's accuracy
            prediction = cur_ks.predict(cur_ue)

            # Update the user's exercise's accuracy model
            cur_ue.accuracy_model().update(label)

            # Update the user's KnowledgeState
            cur_ks.update(cur_ue, problem_type, problem_number, label)

            if prediction == None:
                # None means that there was no model for this exercise
                continue

            # print "%s Prediction/Actual for %s - %d: %.4f / %d" % (kp_file,
            #     exercise, problem_number, prediction, int(label))

            if 'roc_curves' not in model_data:
                model_data['roc_curves'] = {}

            # Initialize this exercises' ROC curve data
            if exercise not in model_data['roc_curves']:
                model_data['roc_curves'][exercise] = {
                    'positive_samples': 0,
                    'negative_samples': 0,
                    'true_positive': np.zeros(thresholds.shape),
                    'true_negative': np.zeros(thresholds.shape),
                    'total_log_liklihood': 0,
                }
            cur_roc = model_data['roc_curves'][exercise]

            # Update this model's ROC curves, and log-likelihood
            if label:
                cur_roc['total_log_liklihood'] += max(np.log(prediction), -100)
                cur_roc['positive_samples'] += 1
            else:
                cur_roc['total_log_liklihood'] += max(
                    np.log(1.0 - prediction), -100)
                cur_roc['negative_samples'] += 1

            for i in range(thresholds.shape[0]):
                if prediction >= thresholds[i]:
                    if label:
                        cur_roc['true_positive'][i] += 1
                    # else: false_positive
                else:
                    if not label:
                        cur_roc['true_negative'][i] += 1
                    # else: false_negative

        # Samples are also assumed to be in user-order, so we purge users when
        # we are done with them to preserve memory
        if prev_user and prev_user != user:
            # Purge all the previous user's exercise state, since we no longer
            # need it.
            user_exercises = {}

            for kp_file, model_data in knowlege_params.iteritems():
                model_data['knowledge_states'] = {}

        prev_user = user

    print "%d of %d (%.2f%%) samples skipped due to ordering problems" % (
        skipped_samples, total_samples,
        (float(skipped_samples) / total_samples) * 100)

    # Plot the ROC curves with matplotlib, group them by exercise
    # Sort them by number of samples
    with pdf.PdfPages(options.graph_pdf) as pdf_file:
        for ex, seen in sorted(seen_exercises.items(),
                key=lambda item: -item[1]):
            legend_names = []
            plt.clf()

            for kp_file, model_data in knowlege_params.iteritems():
                if ex not in model_data['roc_curves']:
                    # print ex, "has no thetas in", kp_file
                    continue

                cur_roc = model_data['roc_curves'][ex]
                positive_samples = float(cur_roc['positive_samples'])
                negative_samples = float(cur_roc['negative_samples'])
                tot_samples = positive_samples + negative_samples

                # TODO(mattfaus): skip if under a minimum barrier
                if (positive_samples < options.min_samples or
                        negative_samples < options.min_samples):
                    # print "Not enough samples for", kp_file
                    continue

                # The following two calculations are NP vectors, so it divides
                # each entry in the vector by the denominator
                true_pos_rate = (cur_roc['true_positive'] / positive_samples)

                # Calculate false positive rate from true negative rate
                false_pos_rate = (([1] * len(cur_roc['true_negative'])) -
                    (cur_roc['true_negative'] / negative_samples))

                # TODO(mattfaus): Remove this AUC calculation
                # Calculate the area under the curve, note that we can't use
                # something like np.trapz() because the sample points in our
                # true_pos_rate/false_pos_rate arrays may overlap.
                # So, first let's fit a quadratic equation to the points:
                fit_line = np.polyfit(false_pos_rate, true_pos_rate, 2)
                # Then, integrate it
                integral = np.polynomial.polynomial.polyint(fit_line[::-1])
                # Then evaluate the integral at x=1
                area_under_curve = np.polynomial.polynomial.polyval(1,
                    integral)

                average_log_liklihood = cur_roc['total_log_liklihood'] / (
                    positive_samples + negative_samples)

                legend_names.append("%s (a-logL=%.3f)" % (
                    os.path.basename(kp_file), average_log_liklihood))

                plt.plot(false_pos_rate, true_pos_rate)

                # Store the calculated ROC and AUC calculations so we can
                # aggregate them later
                cur_roc["true_positive_rate"] = true_pos_rate
                cur_roc["false_positive_rate"] = false_pos_rate
                cur_roc["area_under_curve"] = area_under_curve
                cur_roc["total_samples"] = tot_samples

            if len(legend_names) != len(knowlege_params):
                # Don't draw incomplete graphs
                continue

            # Plot random chance ROC
            plt.plot([0, 1], [0, 1], '--')
            plt.grid()

            plt.xlabel("False positive rate")
            plt.ylabel("True positive rate")
            plt.title("%s (%dp / %dn samples)" % (ex, positive_samples,
                negative_samples))
            plt.legend(legend_names, loc=4)
            pdf_file.savefig()

        # Plot the overall stats
        annotation = ""
        for kp_file, model_data in knowlege_params.iteritems():
            base_name = os.path.basename(kp_file)
            true_pos_rates = [roc["true_positive_rate"]
                for ex, roc in model_data['roc_curves'].iteritems()
                if "true_positive_rate" in roc]
            false_pos_rates = [roc["false_positive_rate"]
                for ex, roc in model_data['roc_curves'].iteritems()
                if "false_positive_rate" in roc]
            aucs = [roc["area_under_curve"]
                for ex, roc in model_data['roc_curves'].iteritems()
                if "area_under_curve" in roc]
            samples = [roc["total_samples"]
                for ex, roc in model_data['roc_curves'].iteritems()
                if "total_samples" in roc]
            tot_log_ls = [roc["total_log_liklihood"]
                for ex, roc in model_data['roc_curves'].iteritems()
                if "total_log_liklihood" in roc]

            # Calculate the average ROC curve
            avg_tpr = np.zeros(thresholds.shape)
            avg_fpr = np.zeros(thresholds.shape)
            avg_auc = 0
            avg_logl = 0
            w_avg_tpr = np.zeros(thresholds.shape)
            w_avg_fpr = np.zeros(thresholds.shape)
            w_avg_auc = 0
            w_avg_logl = 0
            total_samples = sum(samples)
            for i in xrange(len(true_pos_rates)):
                weight = 1.0 / len(true_pos_rates)
                avg_tpr += true_pos_rates[i] * weight
                avg_fpr += false_pos_rates[i] * weight
                avg_auc += aucs[i] * weight
                avg_logl += (tot_log_ls[i] / samples[i]) * weight

                weight = samples[i] / total_samples
                w_avg_tpr += true_pos_rates[i] * weight
                w_avg_fpr += false_pos_rates[i] * weight
                w_avg_auc += aucs[i] * weight
                w_avg_logl += (tot_log_ls[i] / samples[i]) * weight

            # np.where(thresholds == 0.85)[0]
            # import pdb; pdb.set_trace()

            plt.plot(avg_fpr, avg_tpr, label="avg %s (a-logl=%.3f)" % (
                base_name, avg_logl))
            plt.plot(w_avg_fpr, w_avg_tpr, label="w-avg %s (a-logl=%.3f)" % (
                base_name, w_avg_logl))

            w_avg_tpr_85 = w_avg_tpr[np.where(thresholds == 0.85)][0] * 100
            w_avg_fpr_85 = w_avg_fpr[np.where(thresholds == 0.85)][0] * 100
            w_avg_tpr_95 = w_avg_tpr[np.where(thresholds == 0.95)][0] * 100
            w_avg_fpr_95 = w_avg_fpr[np.where(thresholds == 0.95)][0] * 100

            annotation = ("%s%s\n"
                "%.2f%% tpr @ 85%%\n"
                "%.2f%% fpr @ 85%%\n"
                "%.2f%% tpr @ 95%%\n"
                "%.2f%% fpr @ 95%%\n") % (annotation, base_name, w_avg_tpr_85,
                w_avg_fpr_85, w_avg_tpr_95, w_avg_fpr_95)

        # Plot random chance ROC
        plt.plot([0, 1], [0, 1], '--')
        plt.grid()

        plt.annotate(annotation, (0.01, 0.55))

        # For some reason, the auto-scaling doesn't work on these graphs :/
        plt.xlim((0.0, 1.0))
        plt.ylim((0.0, 1.0))
        plt.xlabel("False positive rate")
        plt.ylabel("True positive rate")
        plt.title("Average ROC (%d samples)" % total_samples)
        plt.legend(loc=4)
        pdf_file.savefig()

if __name__ == '__main__':
    main()
