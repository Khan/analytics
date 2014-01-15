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

import ast
import csv
import sys
import time

import matplotlib.pyplot as plt
import numpy as np

# TODO(tony): add command-line args for these?

# folder to store the figures
# TODO(tony): automatically save the figures
FIG_PATH = '~/khan/data/'

# whether or not the computation should be done online
# TODO(tony): compute online for large datasets
ONLINE = False

# whether or not to display the figures
# TODO(tony): implement this
DISPLAY = True


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


def read_data_csv(filename=None):
    data = []
    with (sys.stdin if filename is None else open(filename, 'r')) as f:
        reader = csv.reader(f)
        prev = None
        for row in reader:
            if prev is None:
                prev = csv_to_array(row)
            else:
                row = csv_to_array(row)
                data.append((prev, row))
                prev = None
                if len(data) % 10000 == 0:
                    print '%d processed...' % len(data)
    print 'Users: %d' % len(data)
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
    print 'Users: %d' % len(data)
    return data


def graph_efficiency(n, data, min_problems=0):
    correct = np.zeros(n)
    total = np.zeros(n)
    for task_types, corrects in data:
        m = len(task_types)
        if m < min_problems:
            continue
        assert m <= n
        correct[:m] += corrects
        total[:m] += np.ones(m, dtype=int)
        """
        for i in range(min(n, len(problems))):
            if problems[i][1]:
                correct[i] += 1
            total[i] += 1
        """

    plt.title('Efficiency Curve')
    plt.xlabel('Problem Number')
    plt.ylabel('Percent Correct')

    eff = np.zeros(n)
    for i in range(n):
        if total[i] > 0:
            eff[i] = 1.0 * correct[i] / total[i]
        else:
            eff[i] = 0.0
    plt.plot(eff)
    plt.show()


def graph_efficiency_by_task_type(n, data, min_problems=0):
    correct_by_type = np.zeros((NUM_TYPES, n))
    total_by_type = np.zeros((NUM_TYPES, n))
    for task_types, corrects in data:
        m = len(task_types)
        if m < min_problems:
            continue
        assert m <= n
        for i in xrange(m):
            task_type = task_types[i]
            correct_by_type[task_type][i] += corrects[i]
            total_by_type[task_type][i] += 1

    plt.title('Efficiency Curve: By Task Type')
    plt.xlabel('Problem Number')
    plt.ylabel('Percent Correct')

    for j in xrange(NUM_TYPES):
        if j == NUM_TYPES - 1:
            continue
        correct = correct_by_type[j]
        total = total_by_type[j]
        eff = np.zeros(n)
        for i in xrange(n):
            if total[i] > 0:
                eff[i] = 1.0 * correct[i] / total[i]
            else:
                eff[i] = 0.0
        plt.plot(eff, label=TASK_TYPES[j])

    x1, x2, y1, y2 = plt.axis()
    plt.axis((x1, x2, 0.25, 1.0))
    plt.legend(loc='lower center', ncol=2)
    plt.show()


def graph_engagement(n, data):
    eng = np.zeros(n)
    for l in data:
        eng[:len(l)] += 1

    plt.title('Engagement Curve')
    plt.xlabel('Problem Number')
    plt.ylabel('Number of Users (doing at least x problems)')
    plt.plot(eng)
    plt.show()


def graph_engagement_by_task_type(n, data):
    map_by_type = {}
    for problems in data:
        for i in range(min(n, len(problems))):
            # labels don't like unicode
            task_type = str(problems[i][0])
            if task_type not in map_by_type:
                map_by_type[task_type] = []
            map_by_type[task_type].append(i)

    x = []
    label = []
    for task_type in sorted(map_by_type):
        x.append(map_by_type[task_type])
        label.append(task_type)

    plt.title('Engagement Curve: By Task Type')
    plt.xlabel('Problem Number')
    plt.ylabel('Number of Users (doing at least x problems)')
    plt.hist(x, n, normed=0, histtype='bar', stacked=True, label=label)
    plt.legend()
    plt.show()


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


def graph_analytics_efficiency(eff, eff_max, suffix):
    plt.title('Analytics Cards: Efficiency Curves' + suffix)
    plt.plot(eff, label='Efficiency')
    plt.plot(eff_max, label='Efficiency Max')
    plt.xlabel('Problem Number')
    plt.ylabel('Delta Efficiency')
    plt.legend()
    plt.show()

    plt.title('Analytics Cards: Normalized Efficiency Curve' + suffix)
    plt.plot(normalize_zero(eff, eff_max))
    plt.xlabel('Problem Number')
    plt.ylabel('Delta Efficiency')
    plt.show()


def graph_analytics(n, data):
    counts = []
    first_counts = []
    dist_counts = []

    eff = np.zeros(n)
    eff_max = np.zeros(n)

    eff_all = np.zeros(n)
    eff_all_max = np.zeros(n)

    for problems in data:
        count = 0
        first_index = None
        prev = None
        for i in range(min(n, len(problems))):
            task_type = str(problems[i][0])
            if task_type == 'mastery.analytics':
                count += 1
                if first_index is None:
                    first_index = i
                if prev is not None:
                    dist_counts.append(i - prev)

                    delta = problems[i][1] - problems[prev][1]
                    inv_norm = 1.0 / (i - prev)
                    eff[prev:i] += delta * inv_norm
                    eff_max[prev:i] += inv_norm

                    eff_all[prev:i] += delta
                    eff_all_max[prev:i] += 1
                prev = i
        counts.append(count)
        if first_index is not None:
            first_counts.append(first_index)

    # TODO(tony): make multiple figures
    plt.title('Analytics Cards: Count Distribution')
    plt.hist(counts, n)
    plt.xlabel('Number of Analytics Cards')
    plt.ylabel('Number of Users (with x cards)')
    plt.show()

    plt.title('Analytics Cards: Index of First Card')
    plt.hist(first_counts, n)
    plt.xlabel('Index of First Analytics Card')
    plt.ylabel('Number of Users')
    plt.show()

    plt.title('Analytics Cards: Distance to Next Card')
    plt.hist(dist_counts, n)
    plt.xlabel('Number of Problems Between Analytics Cards')
    plt.ylabel('Number of Instances')
    plt.show()

    graph_analytics_efficiency(eff, eff_max, '')

    graph_analytics_efficiency(eff_all, eff_all_max, ' (Whole Range)')


def graph_and_save_all(n, data):
    # TODO(tony): implement; add prefix/suffix for figure names?
    pass


def main():

    start = time.time()
    n = 100
    data = read_data_csv()
    print 'Done reading input, elapsed: %f' % (time.time() - start)

    min_problems = 0  # 100
    print 'Generating efficiency'
    graph_efficiency(n, data, min_problems)
    print 'Generating efficiency by task type'
    graph_efficiency_by_task_type(n, data, min_problems)
    print 'Done graphing efficiency, elapsed: %f' % (time.time() - start)

    """
    print 'Generating engagement'
    graph_engagement(n, data)
    print 'Generating engagement by task type'
    graph_engagement_by_task_type(n, data)
    print 'Done graphing engagement, elapsed: %f' % (time.time() - start)

    print 'Generating analytics cards stats'
    graph_analytics(n, data)
    print 'Done graphing analytics, elapsed: %f' % (time.time() - start)
    """

if __name__ == '__main__':
    main()
