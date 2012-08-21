#!/usr/bin/env python

"""
percentile_avg.py -l [lower percentile bound] -u [upper percentile bound]

This script calculates the mean of those entries in a column which fall
between a lower and upper percentile bound in the data (trimmed average).
This is particularly useful for outlier removal.  By default
[lower percentile bound] = 0.1 and [upper percentile bound] = 0.9.

Input:
  Column 1: Numerical data to take the trimmed average of.
  Additional Columns (optional): If present, separate trimmed averages will be
    computed over each unique value of the additional columns, rather than
    over the entire dataset.

Output:
  Column 1:  Trimmed average
  Column 2+: The key value the average was taken over.  If there were no
    Additional Columns as input, this will be set to an empty string.
    Otherwise, this will be the value of the additional columns.

Examples:
Take the trimmed average of a single column:
 ADD FILE s3://ka-mapreduce/code/[BRANCH]/py/percentile_avg.py;
 SELECT t.avg FROM (
   SELECT TRANSFORM (
         get_json_object(exercise.json, '$.seconds_per_fast_problem' ) )
     USING 'python percentile_avg.py -l 0.25 -u 0.75'
     AS avg, key
   FROM exercise) t;

 In this use case, the mean is taken over the 25th percentile through 75th
 percentile entries in the column, rather than over all the entries.

Use an additional column as a key:
 ADD FILE s3://ka-mapreduce/code/[BRANCH]/py/percentile_avg.py;
 SELECT TRANSFORM (
       get_json_object(json, '$.seconds_per_fast_problem' ),
       get_json_object(json, '$.author' )
     )
     USING 'python percentile_avg.py -l 0.1 -u 0.9'
     AS avg, key
 FROM
   (SELECT exercise.json as json
    FROM exercise
    DISTRIBUTE BY get_json_object(json, '$.author_name' )
   ) t;


Use two additional columns as a key:
 ADD FILE s3://ka-mapreduce/code/[BRANCH]/py/percentile_avg.py;
 SELECT TRANSFORM (
       get_json_object(json, '$.seconds_per_fast_problem' ),
       get_json_object(json, '$.author' ),
       get_json_object(json, '$.live' )
     )
     USING 'python percentile_avg.py -l 0.1 -u 0.9'
     AS avg, key1, key2
 FROM
   (SELECT exercise.json as json
    FROM exercise
    DISTRIBUTE BY get_json_object(json, '$.author_name' ),
                  get_json_object(json, '$.live' )
   ) t;

"""

import numpy as np
import optparse
import re
import sys


def get_cmd_line_options():
    parser = optparse.OptionParser()
    parser.add_option("-l", "--lower_bound", type=float, default=0.1)
    parser.add_option("-u", "--upper_bound", type=float, default=0.9)
    options, _ = parser.parse_args()
    return options.lower_bound, options.upper_bound


NAN_REGEX = re.compile(r'(^$|\\N)')


def decompose_line(line):
    """Load in a row of input, separating the value and any key columns."""
    line = line.strip('\f\n\r')
    # split on tabs.  only split the first column.  Any remaining
    # columns will be left glommed together as a joint key.
    line_split = line.split('\t', 1)
    val = line_split[0]
    # set a default empty key
    key = ''
    if len(line_split) > 1:
        # if there were two or more columns, set the key to the
        # additional columns
        key = line_split[1]
    # turn empty lines and Hive NaNs into "nan"
    val = NAN_REGEX.sub('nan', val)
    return val, key


def main():
    lower_bound, upper_bound = get_cmd_line_options()

    # load in the data, stripping linefeeds, and converting empty
    # lines and \N to nan
    lines = [decompose_line(line) for line in sys.stdin]

    # turn it into a numpy array
    lines = np.asarray(lines)

    # if there was no input, output Hive's NaN and stop here
    if lines.shape[0] == 0:
        print r'\N'
        return

    # separate the value and key columns. throw an error if we can't convert
    # the value column to floating point
    try:
        vals = lines[:, 0].astype('float')
    except:
        print >>sys.stderr, \
            "percentile_avg.py could not convert first input column to float"
        # and return NaN to Hive
        print r'\N'
        return
    keys = lines[:, 1]

    # throw out any rows with non-finite values
    inds = np.isfinite(vals)
    vals = vals[inds]
    keys = keys[inds]

    # if there was no valid data, output Hive's NaN and stop here
    if vals.shape[0] == 0:
        print r'\N'
        return

    # sort by key
    ord = np.argsort(keys)
    keys = keys[ord]
    vals = vals[ord]

    # get the list of unique keys, and where they occur in the array
    unq_keys, unq_inds = np.unique(keys, return_index=True)
    # step through the unique keys, and output the percentile
    # averaged data for each key
    for ii in range(len(unq_keys)):
        key = unq_keys[ii]
        # the values associated with this key will lie between start_ind and
        # end_ind, since everything is sorted by keys
        start_ind = unq_inds[ii]
        end_ind = vals.shape[0] + 1
        if ii < len(unq_keys) - 1:
            end_ind = unq_inds[ii + 1]
        # get the values data for just this key
        x = vals[start_ind:end_ind]
        # sort it
        x = np.sort(x)

        # average the data only between the appropriate percentiles.  the
        # "if" statements deal with insufficient data.  The behavior if
        # there are only one or two valid rows is inconsistent with that
        # if there's more data, but I think it's better than returning
        # NaNs if there's not enough data
        percentiles = np.arange(x.shape[0], dtype=float)
        if x.shape[0] > 2:
            percentiles = percentiles / np.max(percentiles)
        elif x.shape[0] == 2:
            percentiles[0] = 0.49999
            percentiles[1] = 0.50001
        elif x.shape[0] == 1:
            percentiles[0] = 0.5
        # x_gd holds the elements in x which fall within the allowed percentile
        # range
        x_gd = x[(percentiles >= lower_bound) & (percentiles <= upper_bound)]
        # if there are any elements in the allowed percentile range, average
        # over them, otherwise NaN
        if x_gd.shape[0] > 0:
            avg = np.mean(x_gd)
        else:
            avg = np.nan

        if not np.isfinite(avg):
            # use Hive's NaN string
            print r'\N',
        else:
            # display the average within the selected percentile range
            print avg,
        # and print out the current key
        print "\t%s" % key

if __name__ == '__main__':
    main()






