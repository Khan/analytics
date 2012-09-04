#!/usr/bin/env python

"""Reducer script to generate time series for User Growth trends.

Input: (user, dt, signed_up, videos_started, exercises_started) from
the user_daily_activity table.  See user_growth.q for more details.

Output: (dt, series, value) triples.

Thus, this reducer takes in all the activity for a user, and
emits time series of the account state changes.  Currently, the logic
computes three time series:  joins, deactivations, and reactivations.
"""

import datetime
import sys

g_err_late_join = 0
# start_dt and end_dt define the date range for which to output data.
# end_dt defines the as-of date of the ouput, which is necessary to
# detect when a user history ends in deactivation.
g_start_dt = None
g_end_dt = None

# The following constant parameterizes our definition of an "active" user.
# We define an active user as a user with at least one active visit in the last
# WINDOW_LEN days, where an active visit requires the user to have performed
# an action of watching a video, attempting a problem, or contributing
# discussion or comment feedback.  Instead of calendar months, we use windows
# of 28 days to avoid noise from differing month lengths and weekly seasonality
WINDOW_LEN = 28


def emit_data_point(dt, series, value):
    if dt >= g_start_dt and dt < g_end_dt:
        print "%s\t%s\t%s" % (dt, series, str(value))


def emit_deactivation(last_date):
    deactivation_date = last_date + datetime.timedelta(days=WINDOW_LEN)
    deactivation_dt = deactivation_date.strftime('%Y-%m-%d')
    emit_data_point(deactivation_dt, 'deactivations', 1)


def emit_delta_series(activity):
    global g_err_late_join

    # Loop through daily activity (assume it's sorted by dt)
    last_date = None
    for act in activity:
        user, dt, joined = act

        if joined == 'true':
            emit_data_point(dt, 'joins', 1)
            if last_date:
                # I would not expect the join to be set on any but the
                # first day of activity.  Double check that.
                g_err_late_join += 1

        curr_date = datetime.datetime.strptime(dt, '%Y-%m-%d')

        if last_date and (curr_date - last_date).days > WINDOW_LEN:
            # emit a deactivation and a re-activation
            emit_deactivation(last_date)

            emit_data_point(dt, 'reactivations', 1)

        last_date = curr_date

    # make sure to emit a deactivation if the stream ends with inactivity
    end_date = datetime.datetime.strptime(g_end_dt, '%Y-%m-%d')
    if last_date and (end_date - last_date).days > WINDOW_LEN:
        emit_deactivation(last_date)


def main():

    if len(sys.argv) < 3:
        print >>sys.stderr, "Incorrect usage.  Supply start_dt and end_dt."
        exit(1)

    global g_start_dt, g_end_dt
    g_start_dt, g_end_dt = sys.argv[1:3]

    prev_user = None
    activity = []

    for line in sys.stdin:
        day_activity = tuple(line.rstrip('\n').split('\t'))

        user = day_activity[0]

        if not user:
            continue  # blank user

        if user != prev_user:
            # We're getting a new user, so perform the reduce operation
            # on the current user's activity sequence
            emit_delta_series(activity)
            activity = []

        prev_user = user

        activity.append(day_activity)

    emit_delta_series(activity)

    print >>sys.stderr, "Finished main. %d late join errors." % g_err_late_join

if __name__ == '__main__':
    main()
