"""Bare bones utility script to take an input file or list of input files and
plot ROC curves for each one on a single figure.

Usage:
  cat roc_file | plot_roc_curves.py
    OR
  plot_roc_curves.py *_roc_file

Right now the input files are assumed to be CSV data, with the third column
and the fourth column being the true negative and true positive rates for
some classifier threshold.  Each file contains data for a different curve.

TODO(jace): Maybe take command line args to override the column index
assumption.  But for right now this is simply built to work with output
of accuracy_model_train.py.
"""

import fileinput
import itertools

import matplotlib.pyplot as plt
import numpy as np

lines = ["-+", "--D", "-.s", ":*", "-^", "--|", "-._", ":"]
linecycler = itertools.cycle(lines)


def draw_roc_curve(name, lines):
    lines = [line.strip().split(',') for line in lines]
    lines = np.asarray(lines)

    true_neg = lines[:, 2].astype('float')
    true_pos = lines[:, 3].astype('float')

    # grab the base of the filename
    name = name.split('/')[-1].split('.')[0]

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
