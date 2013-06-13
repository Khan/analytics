"""Bare bones utility script to take an input file or list of input files and
plot ROC curves for each one on a single figure.

Usage:
  cat roc_file | plot_roc_curves.py
    OR
  plot_roc_curves.py *_roc_file

Right now the input files are assumed to be CSV data, with the first column
the correctness on an exercise, and the second column the predicted
probability correct on that exercise.  Each file contains data for a different
curve.

TODO(jace): Maybe take command line args to override the column index
assumption.  But for right now this is simply built to work with output
of accuracy_model_train.py.
"""

import fileinput
import itertools
import warnings

import matplotlib.pyplot as plt
import numpy as np

lines = ["-+", "--D", "-.s", ":*", "-^", "--|", "-._", ":"]
linecycler = itertools.cycle(lines)


def get_correct_predicted(lines):
    lines_split = [line.strip().split(',') for line in lines]
    try:
        lines = np.asarray(lines_split)
        correct = lines[:, 0].astype('float')
        predicted = lines[:, 1].astype('float')
    except:
        # deal with the case where the last row has the wrong number
        # of columns -- eg, if you are looking at a csv file as it's
        # being written
        lines_split = lines_split[:-1]
        lines = np.asarray(lines_split)
        correct = lines[:, 0].astype('float')
        predicted = lines[:, 1].astype('float')

    return correct, predicted


def calc_roc_curve(correct, predicted):
    thresholds = np.arange(-0.01, 1.02, 0.01)
    true_pos = np.zeros(thresholds.shape)
    true_neg = np.zeros(thresholds.shape)
    tot_true = np.max([np.float(np.sum(correct)), 1])
    tot_false = np.max([np.float(np.sum(np.logical_not(correct))), 1])

    for i in range(thresholds.shape[0]):
        threshold = thresholds[i]
        pred1 = predicted >= threshold
        pred0 = predicted < threshold
        if np.sum(tot_true) > 0:
            true_pos[i] = np.sum(correct[pred1]) / tot_true
        if np.sum(tot_false) > 0:
            true_neg[i] = np.sum(np.logical_not(correct[pred0])) / tot_false

    return true_pos, true_neg


def draw_roc_curve(name, lines):
    correct, predicted = get_correct_predicted(lines)
    true_pos, true_neg = calc_roc_curve(correct, predicted)

    # grab the base of the filename
    name = name.split('/')[-1].split('.')[0]

    if name.startswith('_'):
        warnings.warn("Warning.  If name starts with an underscore, "
                      "the label won't display.")

    plt.plot(1 - true_neg, true_pos, next(linecycler), label=name)


def main():
    plt.figure(1)

    lines = []
    filename = None
    for line in fileinput.input():
        if not filename:
            filename = fileinput.filename()
        if fileinput.isfirstline() and len(lines):
            draw_roc_curve(filename, lines)
            filename = fileinput.filename()
            lines = []
        lines.append(line)

    draw_roc_curve(fileinput.filename(), lines)

    plt.xlabel('False Positive Rate')
    plt.ylabel('True Positive Rate')
    plt.title('ROC Curves')
    plt.legend(loc='best')
    plt.xlim([0, 1])
    plt.ylim([0, 1])
    plt.grid()
    plt.show()


if __name__ == '__main__':
    main()
