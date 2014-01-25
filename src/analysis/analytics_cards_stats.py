""" This reads in a csv file of ProblemLog data where each user
is represented by 4 rows, currently:

task_types (encoded as 0..5)
corrects (0 or 1)
exercise (strings)
time_done (unix time in seconds)

each in csv format. We compute a few statistics on analytics cards over time
"""

import argparse
import csv
import datetime
import os
import re
import sys
import time

import matplotlib.pyplot as plt
import numpy as np

# folder to store the figures
FIG_PATH = '~/khan/data/'
FOLDER_NAME = 'tmp'

# whether or not to display the figures
DISPLAY = False

# task types in alphabetical order
TASK_TYPES = (
    'mastery.analytics',
    'mastery.challenge',
    'mastery.coach',
    'mastery.mastery',
    'mastery.review',
    'practice',
)

# number of types
NUM_TYPES = len(TASK_TYPES)


def csv_to_int_array(row):
    return np.array(row, dtype=int)


def csv_to_str_array(row):
    return np.array(row, dtype=str)


def read_data_csv(filename=None):
    data = []
    with (sys.stdin if filename is None else open(filename, 'r')) as f:
        reader = csv.reader(f)
        i = 0
        for line in reader:
            if i == 0:
                types = csv_to_int_array(line)
            elif i == 1:
                corrects = csv_to_int_array(line)
            elif i == 2:
                exercises = csv_to_str_array(line)
            else:
                times = csv_to_int_array(line)
                data.append((types, corrects, exercises, times))
                if len(data) % 10000 == 0:
                    print '%d processed...' % len(data)
            i = (i + 1) & 3
    return data


def filter_for_min_problems(data, min_problems):
    f = lambda (user_problems): len(user_problems[0]) >= min_problems
    return filter(f, data)


def normalize_zero(a, b):
    assert len(a) == len(b)
    n = len(a)
    c = np.zeros(n)
    for i in range(n):
        if b[i] > 0:
            c[i] = a[i] / b[i]
        else:
            c[i] = 0
    return c


def graph_and_save(plot_name, n, min_problems):
    filename = '%s%s/%s_%d_%d.png' % (FIG_PATH, FOLDER_NAME,
        plot_name, n, min_problems)
    filename = os.path.expanduser(filename)
    print 'Saving... %s' % filename
    plt.savefig(filename)
    if DISPLAY:
        plt.show()
    plt.clf()


def unix_time_to_date(ts):
    return datetime.datetime.fromtimestamp(ts).date()


def graph_analytics_by_time(data, n, min_problems=0):
    if min_problems > 0:
        data = filter_for_min_problems(data, min_problems)

    correct = {}
    total = {}
    exercises_by_date = {}
    for task_types, corrects, exercises, times in data:
        m = min(len(task_types), n)
        for i in xrange(m):
            task_type = task_types[i]
            if task_type == 0:  # mastery.analytics
                date = unix_time_to_date(times[i])
                if date in correct:
                    correct[date] += corrects[i]
                    total[date] += 1
                    exercises_by_date[date].add(exercises[i])
                else:
                    correct[date] = corrects[i]
                    total[date] = 1
                    exercises_by_date[date] = set([exercises[i]])

    # second pass for filtered results (use exercises on the first day)
    first_date = sorted(exercises_by_date.keys())[0]
    print 'First Day Exercises: %d\n' % len(exercises_by_date[first_date])
    correct_filtered = {}
    total_filtered = {}
    for task_types, corrects, exercises, times in data:
        m = min(len(task_types), n)
        for i in xrange(m):
            task_type = task_types[i]
            if task_type == 0:  # mastery.analytics
                exercise = exercises[i]
                if exercise not in exercises_by_date[first_date]:
                    continue
                date = unix_time_to_date(times[i])
                if date in correct_filtered:
                    correct_filtered[date] += corrects[i]
                    total_filtered[date] += 1
                else:
                    correct_filtered[date] = corrects[i]
                    total_filtered[date] = 1

    # calculations
    x = np.array(sorted(correct.keys()))
    y = np.array([1.0 * correct[d] / total[d] for d in x])
    y_total = np.array([total[d] for d in x])
    print "Dates:\n%s\n" % x
    print "Accuracy for %s:\n%s\n" % (TASK_TYPES[0], y)
    print "Totals for %s:\n%s\n" % (TASK_TYPES[0], y_total)

    # filtered versions
    x_filtered = np.array(sorted(correct_filtered.keys()))
    y_filtered = np.array([1.0 * correct_filtered[d] / total_filtered[d]
                           for d in x_filtered])
    y_total_filtered = np.array([total_filtered[d] for d in x_filtered])
    # TODO(tony): print this info too

    # accuracy over time
    plt.figure()
    plt.title('Analytics Cards Accuracy '
              '(Min Problems: %d)' % min_problems)
    plt.xlabel('Date')
    plt.ylabel('Percent Correct')

    plt.plot(x, y, label='All Exercises')
    plt.plot(x_filtered, y_filtered, label='First Day Exercises')
    plt.legend()
    graph_and_save('analytics_accuracy', n, min_problems)

    # counts over time
    plt.figure()
    plt.title('Analytics Cards Counts '
              '(Min Problems: %d)' % min_problems)
    plt.xlabel('Date')
    plt.ylabel('Number of Analytics Cards Answered')
    plt.plot(x, y_total, label='All Exercises')
    plt.plot(x_filtered, y_total_filtered, label='First Day Exercises')
    plt.legend()
    graph_and_save('analytics_count', n, min_problems)

    # TODO(tony): add a graph of exercise count over time?


# TODO(tony): refactor and move into a common I/O file?
# TODO(tony): move these files into a learning_gain folder?
def parse_filename(filename):
    if filename is None:
        return 'stdin'
    # try to match start-date_end-date_num-points
    date_pattern = r'(\d+\-\d+\-\d+)'
    num_pattern = r'(\d+)'
    pattern = (r'\D*' + date_pattern
            + r'\_' + date_pattern
            + r'\_' + num_pattern
            + r'\D*')
    match = re.match(pattern, filename)
    # if we do not match, just return the full name
    if not match or sum([g is None for g in match.groups()]):
        return 'output'  # filename
    return '_'.join([match.group(i) for i in range(1, 4)])


def main():
    global FOLDER_NAME

    parser = argparse.ArgumentParser()

    # note: dashes are converted to underscores in property names
    parser.add_argument('-f', '--file',
        help='input file (default is stdin)')
    parser.add_argument('-n', '--num-problems',
        help='number of problems per user to analyze',
        type=int, default=100)
    parser.add_argument('-m', '--min-problems',
        help='minimum number of problems for filtering users',
        type=int, default=0)

    # get arguments
    args = parser.parse_args()

    filename = args.file
    n = args.num_problems
    min_problems = args.min_problems

    # run!
    start = time.time()
    data = read_data_csv(filename)
    print 'Done reading input, elapsed: %f' % (time.time() - start)
    print 'Users: %d' % len(data)
    print 'Users (min_problems=%d): %d' % (min_problems,
        sum([len(t) >= min_problems for t, c, e, d in data]))

    # store output in FIG_PATH/FOLDER_NAME
    FOLDER_NAME = parse_filename(filename)
    directory = os.path.expanduser(FIG_PATH + FOLDER_NAME)
    if not os.path.exists(directory):
        os.makedirs(directory)
        print 'Created directory: %s' % directory
    else:
        print 'Directory exists: %s' % directory

    print 'Generating analytics cards stats'
    graph_analytics_by_time(data, n, min_problems)
    print 'Done graphing analytics, elapsed: %f' % (time.time() - start)


if __name__ == '__main__':
    main()
