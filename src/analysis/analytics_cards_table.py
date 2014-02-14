"""
Generates a conditional probability table by exercise, based on analytics
cards. Specifically, we compute

Pr(X_i = x_i | X_j = x_j),

where i, j represent exercises, X_i, X_j are binary random variables
represnting correctness, and x_i, x_j represent correctness values.

The quantities we are then interested in are

Y_jk = sum_i Pr(X_i = 1 | X_j = k),

i.e. the average accuracy on analytics cards given we answered exercise j
with correctness k.
"""

import argparse
import csv
import sys
import time

import numpy as np


def read_data(filename):
    data = []
    with (sys.stdin if filename is None else open(filename, 'r')) as f:
        reader = csv.reader(f)
        header = reader.next()

        # because I keep forgetting which order the rows are in
        user_id_index = header.index('user_id')
        exercise_index = header.index('exercise')
        correct_index = header.index('correct')

        # break down analytics cards by user_id
        prev_user_id = None
        for row in reader:
            user_id = row[user_id_index]
            exercise = row[exercise_index]
            correct = bool(row[correct_index])
            if prev_user_id is None or user_id != prev_user_id:
                prev_user_id = user_id
                data.append([])
            data[-1].append((exercise, correct))

    return data


def get_exercises(data, min_problems):
    exercise_cnt = {}
    for row in data:
        for e, c in row:
            if e not in exercise_cnt:
                exercise_cnt[e] = 1
            else:
                exercise_cnt[e] += 1

    # filter out the retired/experimental exercises
    exercises = []
    for e in exercise_cnt:
        cnt = exercise_cnt[e]
        if cnt >= min_problems:
            exercises.append(e)
    exercises.sort()

    return exercises


def compute_and_write(data, min_problems, filename):
    """ Write out the results. Each row is given by:

    exercise,prob0,prob1.

    Here, probk is the average probability of getting analytics card right,
    given that we got correct == k on the current exercise.
    """
    exercises = get_exercises(data, min_problems)
    exercise_to_index = {}
    for i, e in enumerate(exercises):
        exercise_to_index[e] = i

    n = len(exercises)
    print 'Num exercises:', n

    # pk[i][0][j] / pk[i][1][j] is Pr(X_j = 1 | X_i = k)
    # the two middle indicies 0 and 1 represent the number correct and total
    p0 = np.zeros((n, 2, n))
    p1 = np.zeros((n, 2, n))

    for row in data:
        prev_e, prev_c = None, False
        for e, c in row:
            # skip exercises that were filtered out
            if e not in exercise_to_index:
                continue

            # look at our current pair!
            if prev_e:
                i = exercise_to_index[prev_e]
                j = exercise_to_index[e]
                if prev_c:
                    p1[i][0][j] += c
                    p1[i][1][j] += 1
                else:
                    p0[i][0][j] += c
                    p0[i][1][j] += 1
            prev_e, prev_c = e, c

    # TODO(tony): plot overall exercise accuracy by these values?
    with open(filename, 'w') as f:
        for i, e in enumerate(exercises):
            prob0 = np.mean(p0[i][0] / p0[i][1])
            prob1 = np.mean(p1[i][0] / p1[i][1])
            f.write('%s,%.5f,%.5f\n' % (e, prob0, prob1))


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument('-f', '--file',
        help='input file (default is stdin)')
    parser.add_argument('-m', '--min-problems',
        help='minimum number of samples for filtering exercises',
        type=int, default=1000)

    args = parser.parse_args()

    filename = args.file
    min_problems = args.min_problems

    # run!
    start = time.time()
    data = read_data(filename)
    print 'Done reading input, elapsed: %f' % (time.time() - start)
    compute_and_write(data, min_problems, 'table.csv')
    print 'Done computing and writing, elapsed: %f' % (time.time() - start)


if __name__ == '__main__':
    main()
