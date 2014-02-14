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


def get_exercises(data):
    exercise_cnt = {}
    for row in data:
        for exercise, correct in row:
            if exercise not in exercise_cnt:
                exercise_cnt[exercise] = 1
            else:
                exercise_cnt[exercise] += 1
    return exercise_cnt


def compute_and_write(data, min_problems, filename):
    """ Write out the results. Each row is given by:

    exercise,prob_0,prob_1.

    Here, prob_k is the average probability of getting analytics card right,
    given that we got correct == k on the current exercise.
    """
    exercises = get_exercises(data)
    n = len(exercises)
    print 'Num exercises:', n
    p = np.zeros((n, n))
    return p


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument('-f', '--file',
        help='input file (default is stdin)')
    parser.add_argument('-m', '--min-problems',
        help='minimum number of samples for filtering exercises',
        type=int, default=10000)

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
