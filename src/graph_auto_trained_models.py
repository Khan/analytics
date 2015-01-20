#!/usr/bin/env python
"""
This script is a rewrite of compare_accuracy_models.py so that it will work
for performance files output by the automated knowledge model retraining
process available in webapp/predictions/pipelines.py.

To use this script,

1) find a recently run parameter training job under
https://console.developers.google.com/project/124072386181/storage/browser/ka_prediction_data/

2) Download the performance data with gsutil, like:
    gsutil cp gs://ka_prediction_data/classic-2014_12_05-a97c65107cd411e4aeda1e7f2780d096/performance_data /tmp  # @Nolint

3) Graph the performance data with this script
    graph_auto_trained_models.py --data /tmp

4) Open the resulting PDF
    open /tmp/graphs.pdf

Furthermore, this tool is meant to handle multiple jobs worth of performance
data so that jobs may be compared against one another. To do that, simply
copy all performance data into the same directory and run this script against
the merged directory.
"""
import argparse
import copy
import itertools
import json
import os
import os.path

import numpy

import matplotlib

# Improve cross-system compatability for writing PDFs with this call.
# Note, it must be called before pyplot and backend_pdf are imported.
matplotlib.use('Agg')

import matplotlib.pyplot as plt
import matplotlib.backends.backend_pdf as pdf


# The minimum number of samples required to plot the curve
MINIMUM_SAMPLES = 1000


def parse_command_line():
    parser = argparse.ArgumentParser(description='Graph performance data.')

    parser.add_argument("--data", help="Path to performance_data.json files")

    options = parser.parse_args()

    return options


def deserialize_file(perf_file_path):
    """
    Parses a file with format:
        <exercise_1> {'job_id': {<performance_data>}}
        <exercise_2> {'job_id': {<performance_data>}}
        ...
        <exercise_N> {'job_id': {<performance_data>}}

    Returns: {
        'exercise_1': {
            'job_id': {
                <performance_data>
            }
        }
        ...
        'exercise_N': ...

        The format of <performance_data> can be found in
        webapp/prediction/pipelines.py~update_knowledge_model_performance().
    }

    """
    results = {}
    with open(perf_file_path, 'r') as perf_file:
        for line in perf_file.readlines():
            json_start = line.index(' {')
            exercise = line[:json_start]
            json_data = line[json_start:]
            try:
                perf_data = json.loads(json_data)
            except ValueError:
                print 'Could not decode performance data for %s: %s' % (
                    exercise, line)
                continue

            # Note: We assume a single performance_data file only has results
            # from a single job.
            results[exercise] = perf_data

    return results


def read_all_performance_data(data_path):
    """Reads all performance data from a directory.

    Returns: {
        'exercise_1': {
            'job_id': {
                <performance_data>
            },
            'job_id2': {...},
            ...
            'job_idN': {...},

        }
        ...
        'exercise_N': ...
    }
    """
    all_perf_data = {}

    for data_file in os.listdir(data_path):
        path = os.path.join(data_path, data_file)
        if data_file.endswith(".json"):
            perf_data = deserialize_file(path)

            # Intelligently merge in performance data, such that data from
            # different jobs is picked up correctly
            for exercise in perf_data.keys():
                if exercise not in all_perf_data:
                    all_perf_data[exercise] = perf_data[exercise]
                else:
                    cur_job_id = perf_data[exercise].keys()[0]
                    if cur_job_id not in set(all_perf_data[exercise].keys()):
                        all_perf_data[exercise][cur_job_id] = (
                            perf_data[exercise][cur_job_id])
                    else:
                        print ("Ignoring second instance of data from job "
                            "%s for exercise %s" % (cur_job_id, exercise))

    # Print some summary statistics about the performance data read
    all_job_ids = [p.keys() for p in all_perf_data.values()]
    total_exercises = len(all_perf_data.keys())
    print 'Loaded performance data for %d exercises across %d jobs.' % (
        total_exercises,
        len(set(itertools.chain(*all_job_ids))))

    all_overall = {}
    for exercise, job_data in all_perf_data.iteritems():
        for job_id, perf_data in job_data.iteritems():
            all_overall = _merge_performance_data(all_overall, perf_data)

    _compute_curve_data(all_overall['prediction'])
    _compute_curve_data(all_overall['original_prediction'])

    keys_to_delete = ('true_positive', 'true_positive_rate', 'true_negative',
        'false_positive_rate')

    for data_set in ('prediction', 'original_prediction'):
        for del_key in keys_to_delete:
            del all_overall[data_set][del_key]

    all_overall['prediction']['avg_positive_samples'] = (
        all_overall['prediction']['positive_samples'] / total_exercises)
    all_overall['prediction']['avg_negative_samples'] = (
        all_overall['prediction']['negative_samples'] / total_exercises)
    all_overall['original_prediction']['avg_positive_samples'] = (
        all_overall['prediction']['positive_samples'] / total_exercises)
    all_overall['original_prediction']['avg_negative_samples'] = (
        all_overall['prediction']['negative_samples'] / total_exercises)

    print "Summary of all performance data:"
    print json.dumps(all_overall, indent=4)

    return all_perf_data


def _plot_perf_data(name, perf_data):
    """Calls plt.plot() for the original and new prediction data."""
    new_plotted = False
    if ('prediction' in perf_data
        and perf_data['prediction']['positive_samples']
            + perf_data['prediction']['negative_samples']
            > MINIMUM_SAMPLES
        and perf_data['prediction']['positive_samples']
            * perf_data['prediction']['negative_samples']
            != 0):
        # Draw this curve
        plt.plot(perf_data['prediction']['false_positive_rate'],
            perf_data['prediction']['true_positive_rate'],
            label="%s new (%ds %0.3f alogl)" % (
            name,
            perf_data['prediction']['positive_samples']
            + perf_data['prediction']['negative_samples'],
            perf_data['prediction']['avg_log_liklihood']))
        new_plotted = True

    orig_plotted = False
    if ('original_prediction' in perf_data
        and perf_data['original_prediction']['positive_samples']
            + perf_data['original_prediction']['negative_samples']
            > MINIMUM_SAMPLES
        and perf_data['original_prediction']['positive_samples']
            * perf_data['original_prediction']['negative_samples']
            != 0):
        # Draw this curve
        plt.plot(
        perf_data['original_prediction']['false_positive_rate'],
        perf_data['original_prediction']['true_positive_rate'],
        label="%s orig (%ds %0.3f alogl)" % (
            name,
            perf_data['original_prediction']['positive_samples']
            + perf_data['original_prediction']['negative_samples'],
            perf_data['original_prediction']['avg_log_liklihood']))
        orig_plotted = True

    return new_plotted, orig_plotted


def _plot_chance_and_save(pdf_file, title):
    plt.plot([0, 1], [0, 1], '--')
    plt.grid()

    plt.xlim((0.0, 1.0))
    plt.ylim((0.0, 1.0))
    plt.xlabel("False positive rate")
    plt.ylabel("True positive rate")
    plt.title(title)
    plt.legend(loc=4)
    pdf_file.savefig()

    # Prepare for new page
    plt.clf()


def _compute_curve_data(data):
    """Converts a raw perf data dict into the corresponding numpy primitives.
    Computes secondary statistics, like the true_positive_rate and
    avg_log_liklihood.
    """
    data['positive_samples'] = float(data['positive_samples']) or 0.00001
    data['negative_samples'] = float(data['negative_samples']) or 0.00001

    # Translate true_positive / true_negative vectors into
    # their corresponding rate equivalents
    data['true_positive'] = numpy.array(data['true_positive'])
    data['true_positive_rate'] = (
        data['true_positive'] / data['positive_samples'])

    data['true_negative'] = numpy.array(data['true_negative'])
    data['false_positive_rate'] = (
        [1] * len(data['true_negative']) -
        (data['true_negative'] / data['negative_samples']))

    data['total_log_liklihood'] = float(data['total_log_liklihood'])
    if data['positive_samples'] + data['negative_samples'] > 0:
        data['avg_log_liklihood'] = data['total_log_liklihood'] / (
            data['positive_samples'] + data['negative_samples'])
    else:
        data['avg_log_liklihood'] = -1


def _merge_performance_data(running_data, new_data):
    """Utility function for merging individual performance data into an
    aggregate dictionary so that aggregate graphs can be computed.
    """
    if not running_data:
        return new_data

    def _merge_inner_perf_data(r, n):
        if not r:
            return n

        return {
            'positive_samples': r['positive_samples'] + n['positive_samples'],
            'negative_samples': r['negative_samples'] + n['negative_samples'],
            'true_positive': numpy.sum(
                [r['true_positive'], n['true_positive']], axis=0),
            'true_negative': numpy.sum(
                [r['true_negative'], n['true_negative']], axis=0),
            'total_log_liklihood': (
                r['total_log_liklihood'] + n['total_log_liklihood']),
            # Note: These two members must be re-computed after all merging
            # is done, so we set them to 0's here.
            'true_positive_rate': numpy.zeros(
                numpy.shape(n['true_positive_rate'])),
            'false_positive_rate': numpy.zeros(
                numpy.shape(n['true_positive_rate'])),
            'avg_log_liklihood': 0.0,
        }

    to_return = {}
    if 'prediction' in new_data:
        if 'avg_log_liklihood' not in new_data['prediction']:
            _compute_curve_data(new_data['prediction'])

        to_return['prediction'] = _merge_inner_perf_data(
            running_data.get('prediction'), new_data['prediction'])

    if 'original_prediction' in new_data:
        if 'avg_log_liklihood' not in new_data['original_prediction']:
            _compute_curve_data(new_data['original_prediction'])

        to_return['original_prediction'] = _merge_inner_perf_data(
            running_data.get('original_prediction'),
            new_data['original_prediction'])

    return to_return


def generate_pdf(all_perf_data, pdf_path):
    """Iterates over all performance data and outputs a PDF full of graphs."""

    # Sort exercises by total samples, so more frequent exercises are first
    exercise_items = all_perf_data.items()

    def total_samples(item):
        exercise, perf_data = item
        return -sum(v['prediction']['positive_samples']
            + v['prediction']['negative_samples']
            for k, v in perf_data.iteritems())

    exercise_items.sort(key=total_samples)

    with pdf.PdfPages(pdf_path) as pdf_file:

        orig_vs_new_overall = {}
        only_new_overall = {}
        all_new_overall = {}

        # Each exercise may have many different jobs with performance data
        # that we'll want to graph.
        for exercise, jobs in exercise_items:
            something_plotted = False

            for job, perf_data in jobs.iteritems():

                # Iterate over each exercise, compute and draw the ROC curve
                _compute_curve_data(perf_data['prediction'])
                _compute_curve_data(perf_data['original_prediction'])
                new_plotted, orig_plotted = _plot_perf_data(job[:5], perf_data)

                something_plotted = (something_plotted or new_plotted
                    or orig_plotted)

                # Update the various aggregate curve data
                if new_plotted:
                    to_merge = copy.deepcopy(perf_data)
                    del to_merge['original_prediction']
                    all_new_overall[job] = _merge_performance_data(
                        all_new_overall.get(job), to_merge)

                if new_plotted and orig_plotted:
                    orig_vs_new_overall[job] = _merge_performance_data(
                        orig_vs_new_overall.get(job), perf_data)
                else:
                    to_merge = copy.deepcopy(perf_data)
                    del to_merge['original_prediction']
                    only_new_overall[job] = _merge_performance_data(
                        only_new_overall.get(job), to_merge)

            if something_plotted:
                _plot_chance_and_save(pdf_file, exercise)

        # Draw the total ROC curves
        # Original vs. New only for exercises that had original
        for job, perf_data in orig_vs_new_overall.iteritems():
            _compute_curve_data(perf_data['prediction'])
            _compute_curve_data(perf_data['original_prediction'])
            _plot_perf_data(job[:5], perf_data)
            _plot_chance_and_save(pdf_file, "Original vs. New")

        # New for exercises that didn't have original
        for job, perf_data in only_new_overall.iteritems():
            _compute_curve_data(perf_data['prediction'])
            _plot_perf_data(job[:5], perf_data)
            _plot_chance_and_save(pdf_file, "Only New")

        # All New
        for job, perf_data in all_new_overall.iteritems():
            _compute_curve_data(perf_data['prediction'])
            _plot_perf_data(job[:5], perf_data)
            _plot_chance_and_save(pdf_file, "All New")


def main():
    options = parse_command_line()

    all_perf_data = read_all_performance_data(options.data)

    output_path = os.path.join(options.data, 'graphs.pdf')
    generate_pdf(all_perf_data, output_path)


if __name__ == "__main__":
    main()
