""" This reads in a csv file of ProblemLog data, e.g.:

5,5,0
0,0,1
1,2
1,0
...

where each pair of rows is a list of task_type followed by a list of correct
for ProblemLogs ordered by time_done. This format is subject to change.

We compute a few statistics on the distribution of analytics cards (among
other things).
"""

import argparse
import ast
import csv
import os
import re
import random
import sys
import time

import matplotlib.pyplot as plt
import numpy as np

# folder to store the figures
# TODO(tony): automatically save the figures
FIG_PATH = '~/khan/data/'
FOLDER_NAME = 'tmp'

# whether or not the computation should be done online
# TODO(tony): compute online for large datasets
ONLINE = False

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

# number of types (including None)
NUM_TYPES = len(TASK_TYPES) + 1


def csv_to_array(row):
    return np.array(row, dtype=int)


def read_data_csv(filename=None, num_rows=2):
    data = []
    with (sys.stdin if filename is None else open(filename, 'r')) as f:
        reader = csv.reader(f)
        i = 0
        for row in reader:
            if i % num_rows == 0:
                prev = csv_to_array(row)
            elif i % num_rows == 1:
                row = csv_to_array(row)
                data.append((prev, row))
                if len(data) % 10000 == 0:
                    print '%d processed...' % len(data)
            i = (i + 1) % num_rows
    return data


def read_data_list(filename=None):
    if filename is None:
        f = sys.stdin
    else:
        f = open(filename, 'r')

    data = []
    for row in f:
        # skip empty rows '[]\n'
        if len(row) <= 3:
            continue
        problems = ast.literal_eval(row)
        data.append(problems)
        if len(data) % 10000 == 0:
            print '%d processed...' % len(data)
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


def graph_accuracy(data, n, min_problems=0):
    correct = np.zeros(n)
    total = np.zeros(n)
    for task_types, corrects in data:
        m = min(len(task_types), n)
        if m < min_problems:
            continue
        correct[:m] += corrects
        total[:m] += np.ones(m, dtype=int)

    plt.title('Accuracy Curve')
    plt.xlabel('Problem Number')
    plt.ylabel('Percent Correct')

    acc = normalize_zero(correct, total)
    plt.plot(acc)
    plt.show()


def graph_accuracy_by_task_type(data, n, min_problems=0, print_data=False):
    correct_by_type = np.zeros((NUM_TYPES, n))
    total_by_type = np.zeros((NUM_TYPES, n))
    for task_types, corrects in data:
        m = min(len(task_types), n)
        if m < min_problems:
            continue
        for i in xrange(m):
            task_type = task_types[i]
            correct_by_type[task_type][i] += corrects[i]
            total_by_type[task_type][i] += 1

    plt.title('Accuracy Curve: By Task Type')
    plt.xlabel('Problem Number')
    plt.ylabel('Percent Correct')

    for j in xrange(NUM_TYPES):
        if j == NUM_TYPES - 1:
            continue
        correct = correct_by_type[j]
        total = total_by_type[j]
        acc = normalize_zero(correct, total)
        plt.plot(acc, label=TASK_TYPES[j])
        if print_data:
            print "Accuracy for %s:\n%s\n" % (TASK_TYPES[j], acc)

    x1, x2, y1, y2 = plt.axis()
    plt.axis((x1, x2, 0.25, 1.0))
    plt.legend(loc='lower center', ncol=2)
    plt.show()


def graph_engagement(data, n):
    eng = np.zeros(n)
    for t, c in data:
        eng[:min(len(t), n)] += 1

    plt.title('Engagement Curve')
    plt.xlabel('Problem Number')
    plt.ylabel('Number of Users (doing at least x problems)')
    plt.plot(eng)
    plt.show()


def graph_engagement_by_task_type(data, n, min_problems=0):
    eng_by_type = [[] for i in xrange(NUM_TYPES)]
    for task_types, corrects in data:
        m = min(len(task_types), n)
        if m < min_problems:
            continue
        for i in xrange(m):
            task_type = task_types[i]
            eng_by_type[task_type].append(i)

    x = []
    label = []
    for i in xrange(NUM_TYPES):
        x.append(eng_by_type[i])
        label.append('None' if i + 1 == NUM_TYPES else TASK_TYPES[i])

    plt.figure()
    plt.title('Engagement Curve (Min Problems: %d): '
              'By Task Type' % min_problems)
    plt.xlabel('Problem Number')
    plt.ylabel('Number of Users (doing at least x problems)')
    plt.hist(x, n, normed=0, histtype='bar', stacked=True, label=label)
    plt.legend()
    graph_and_save('engagement', n, min_problems)


def graph_engagement_ratio(data, n, min_problems=0):
    eng = np.zeros((NUM_TYPES, n))
    for task_types, corrects in data:
        m = min(len(task_types), n)
        if m < min_problems:
            continue
        for i in xrange(m):
            eng[task_types[i]][i] += 1

    # exclude None types here
    cnt_analytics = eng[0]
    cnt_mastery = np.sum(eng[:-2], axis=0)
    cnt_total = np.sum(eng[:-1], axis=0)

    plt.figure()
    plt.title('Engagement Ratios (Min Problems: %d)' % min_problems)
    plt.xlabel('Problem Number')
    plt.ylabel('Ratio Between Problem Types')
    plt.plot(cnt_analytics / cnt_mastery, label='analytics/mastery')
    plt.plot(cnt_analytics / cnt_total, label='analytics/all')
    plt.plot(cnt_mastery / cnt_total, label='mastery/all')
    plt.legend(loc='center right', ncol=1)
    plt.legend()
    graph_and_save('engagement_ratio', n, min_problems)


def graph_analytics_efficiency(eff, eff_max, suffix='', file_suffix='',
                               min_problems=0):
    plt.figure()
    plt.title('Analytics Cards: Efficiency Curves' + suffix)
    plt.plot(eff, label='Efficiency')
    plt.plot(eff_max, label='Efficiency Max')
    plt.xlabel('Problem Number')
    plt.ylabel('Delta Efficiency')
    plt.legend()
    graph_and_save('analytics-eff' + file_suffix, len(eff), min_problems)

    plt.figure()
    plt.title('Analytics Cards: Normalized Efficiency Curve' + suffix)
    plt.plot(normalize_zero(eff, eff_max))
    plt.xlabel('Problem Number')
    plt.ylabel('Delta Efficiency')
    graph_and_save('analytics-eff' + file_suffix + '-norm', len(eff),
                   min_problems)

    plt.figure()
    plt.title('Analytics Cards: Cumulative Normalized Efficiency Curve'
              + suffix)
    plt.plot(np.cumsum(normalize_zero(eff, eff_max)))
    plt.xlabel('Problem Number')
    plt.ylabel('Cumulative Efficiency')
    graph_and_save('analytics-eff' + file_suffix + '-norm-total', len(eff),
                   min_problems)


def graph_learning_gain(eff, eff_max, eng, suffix='', file_suffix='',
                        min_problems=0):
    plt.figure()
    plt.title('Cumulative Learning Gain Curve' + suffix)
    plt.plot(np.cumsum(eng * normalize_zero(eff, eff_max)))
    plt.xlabel('Problem Number')
    plt.ylabel('Learning Gain')
    graph_and_save('learning-gain' + file_suffix, len(eff), min_problems)

    plt.figure()
    plt.title('Cumulative Learning Gain Curve (No Norm)' + suffix)
    plt.plot(np.cumsum(eng * eff))
    plt.xlabel('Problem Number')
    plt.ylabel('Learning Gain')
    graph_and_save('learning-gain-no-norm' + file_suffix, len(eff),
                   min_problems)


def graph_analytics(data, n, min_problems=0):
    if min_problems > 0:
        data = filter_for_min_problems(data, min_problems)

    counts = []
    first_counts = []
    dist_counts = []
    dist_by_delta = [[] for i in xrange(4)]
    delta_by_dist = np.zeros((4, n))

    eff = np.zeros(n)
    eff_max = np.zeros(n)
    eng = np.zeros(n)

    eff_all = np.zeros(n)
    eff_all_max = np.zeros(n)

    for task_types, corrects in data:
        count = 0
        first_index = None
        prev = None
        m = min(len(task_types), n)
        eng[:m] += 1
        for i in xrange(m):
            if task_types[i] == 0:  # corresponds to mastery.analytics
                count += 1
                if first_index is None:
                    first_index = i
                if prev is not None:
                    dist_counts.append(i - prev)

                    delta = corrects[i] - corrects[prev]
                    inv_norm = 1.0 / (i - prev)
                    eff[prev:i] += delta * inv_norm
                    eff_max[prev:i] += inv_norm

                    eff_all[prev:i] += delta
                    eff_all_max[prev:i] += 1

                    mask = 2 * corrects[i] + corrects[prev]
                    dist_by_delta[mask].append(i - prev)
                    delta_by_dist[mask][i - prev] += 1
                prev = i
        counts.append(count)
        if first_index is not None:
            first_counts.append(first_index)

    """
    plt.figure()
    plt.title('Analytics Cards: Count Distribution')
    plt.hist(counts, n)
    plt.xlabel('Number of Analytics Cards')
    plt.ylabel('Number of Users (with x cards)')
    graph_and_save('analytics-count', n, min_problems)

    plt.figure()
    plt.title('Analytics Cards: Index of First Card')
    plt.hist(first_counts, n)
    plt.xlabel('Index of First Analytics Card')
    plt.ylabel('Number of Users')
    graph_and_save('analytics-first', n, min_problems)

    plt.figure()
    plt.title('Analytics Cards: Distance to Next Card')
    plt.hist(dist_counts, n)
    plt.xlabel('Number of Problems Between Analytics Cards')
    plt.ylabel('Number of Instances')
    graph_and_save('analytics-dist-next', n, min_problems)
    """

    # delta and dist distributions
    delta_labels = ('0-0', '0-1', '1-0', '1-1')
    plt.figure()
    plt.title('Analytics Cards: Delta by Distance (Counts)')
    plt.xlabel('Number of Problems Between Analytics Cards')
    plt.ylabel('Instances With Given Delta')
    plt.hist(dist_by_delta, n, normed=0, histtype='bar', stacked=True,
             label=delta_labels)
    plt.legend()
    graph_and_save('analytics-delta-by-dist-cnt', n, min_problems)

    plt.figure()
    plt.title('Analytics Cards: Delta by Distance (Percentage)')
    plt.xlabel('Number of Problems Between Analytics Cards')
    plt.ylabel('Percentage with Given Delta')
    for mask in xrange(4):
        plt.plot(normalize_zero(delta_by_dist[mask],
                                np.sum(delta_by_dist, axis=0)),
                 label=delta_labels[mask])
    plt.legend()
    graph_and_save('analytics-delta-by-dist-pct', n, min_problems)

    # efficiency and learning gain
    graph_analytics_efficiency(eff, eff_max, min_problems=min_problems)
    graph_analytics_efficiency(eff_all, eff_all_max,
                               ' (Whole Range)', '-whole',
                               min_problems=min_problems)
    graph_analytics_efficiency(eff, eff_all_max,
                               ' (Mixed)', '-mixed',
                               min_problems=min_problems)
    graph_learning_gain(eff, eff_max, eng, min_problems=min_problems)
    graph_learning_gain(eff_all, eff_all_max, eng,
                        ' (Whole Range)', '-whole',
                        min_problems=min_problems)
    graph_learning_gain(eff, eff_all_max, eng,
                        ' (Mixed)', '-mixed',
                        min_problems=min_problems)


def graph_analytics_accuracy(data, n, min_problems=0):
    if min_problems > 0:
        data = filter_for_min_problems(data, min_problems)

    correct = np.zeros(n)
    total = np.zeros(n)
    for task_types, corrects in data:
        m = min(len(task_types), n)
        for i in xrange(m):
            task_type = task_types[i]
            if task_type == 0:  # mastery.analytics
                correct[i] += corrects[i]
                total[i] += 1

    plt.title('Analytics Cards Accuracy '
              '(Min Problems: %d)' % min_problems)
    plt.xlabel('Problem Number')
    plt.ylabel('Percent Correct')

    acc = normalize_zero(correct, total)
    print "Accuracy for %s:\n%s\n" % (TASK_TYPES[0], acc)
    print "Totals for %s:\n%s\n" % (TASK_TYPES[0], total)
    # TODO(tony): add trendline, R^2, etc?
    plt.plot(acc)
    graph_and_save('analytics_accuracy', n, min_problems)


def graph_analytics_multi_sample(data, n, min_problems=0, num_samples=5,
                                 sample_ratio=.5):
    analytics_data = []
    for task_types, corrects in data:
        m = min(len(task_types), n)
        cards = []
        for i in xrange(m):
            if task_types[i] == 0:  # mastery.analytics
                cards.append((i, corrects[i]))
        if len(cards) >= 2:
            analytics_data.append(cards)
    print 'Users with at least 2 analytics cards: %d' % len(analytics_data)

    random.seed(0)
    sample_size = int(sample_ratio * len(analytics_data))

    plt.figure()
    for j in xrange(num_samples):
        eff = np.zeros(n)
        eff_max = np.zeros(n)
        if j == 0:  # use the first sample as all data
            sample = analytics_data
        else:
            sample = random.sample(analytics_data, sample_size)
        for cards in sample:
            for i in xrange(1, len(cards)):
                prev = cards[i - 1][0]
                cur = cards[i][0]
                delta = cards[i][1] - cards[i - 1][1]
                inv_norm = 1.0 / (cur - prev)
                eff[prev:cur] += delta * inv_norm
                eff_max[prev:cur] += 1
        plt.plot(np.cumsum(normalize_zero(eff, eff_max)), label=str(j))
    plt.title('Cumulative Normalized Efficiency\n'
              'Sample Ratio: %.2f' % sample_ratio)
    plt.xlabel('Problem Number')
    plt.ylabel('Cumulative Delta Efficiency')
    plt.legend()
    graph_and_save('eff-total', n, min_problems)


def graph_and_save_all(data, n, min_problems=0):
    # TODO(tony): implement; add prefix/suffix for figure names?
    pass


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
    data = read_data_csv(filename, 2)
    print 'Done reading input, elapsed: %f' % (time.time() - start)
    print 'Users: %d' % len(data)
    print 'Users (min_problems=%d): %d' % (min_problems,
        sum([len(t) >= min_problems for t, c in data]))

    # store output in FIG_PATH/FOLDER_NAME
    FOLDER_NAME = parse_filename(filename)
    directory = os.path.expanduser(FIG_PATH + FOLDER_NAME)
    if not os.path.exists(directory):
        os.makedirs(directory)
        print 'Created directory: %s' % directory
    else:
        print 'Directory exists: %s' % directory

    # TODO(tony): make the graph choice a command-line arg (or args?)

    """
    print 'Generating accuracy'
    graph_accuracy(data, n, min_problems)
    print 'Generating accuracy by task type'
    graph_accuracy_by_task_type(data, n, 0, True)
    print 'Done graphing accuracy, elapsed: %f' % (time.time() - start)

    print 'Generating engagement'
    graph_engagement(data, n)
    print 'Generating engagement by task type'
    graph_engagement_by_task_type(data, n)
    graph_engagement_by_task_type(data, n, min_problems)
    graph_engagement_ratio(data, n, min_problems)
    print 'Done graphing engagement, elapsed: %f' % (time.time() - start)
    """

    print 'Generating analytics cards stats'
    # graph_analytics(data, n, min_problems)
    # graph_analytics_accuracy(data, n, min_problems)
    graph_analytics_multi_sample(data, n, min_problems, 5, 0.5)
    print 'Done graphing analytics, elapsed: %f' % (time.time() - start)

if __name__ == '__main__':
    main()
