#!/usr/bin/python
"""
Reduce (user, coach, joined_on) tuple to produce
    summary of teachers and students using the site

Produces (<coach|student>, date) tuples
"""

import sys
import datetime
import time


def daterange(start_date, end_date):
    """Date range iterator Yields consecutive dates
        between start_date inclusive and end_date exclusive
    """
    for n in range(int((end_date - start_date).days)):
        yield start_date + datetime.timedelta(n)


def emit_data_row(value, date):
    """Create data row. Friendly wrapper"""

    emit_str = "{0}\t{1}".format(value, date)
    print(emit_str)


def fill_value(value, from_dt, to_dt):
    """Replicate given value from from_dt inclusive to to_dt exclusive"""

    from_date = datetime.datetime.strptime(from_dt, '%Y-%m-%d').date()
    to_date = datetime.datetime.strptime(to_dt, '%Y-%m-%d').date()

    for single_date in daterange(from_date, to_date):
        date_string = time.strftime("%Y-%m-%d", single_date.timetuple())
        emit_data_row(value, date_string)


def reduce_coaches(end_date):
    """Process input lines and determine whether given coach is a teacher
    Fills in missing spots with values from preceeding dates
    Assumes sortedness of data, i.e. data for single coach
        should be date ordered and clustered together.
    """

    current_coach = ""
    num_students = 0

    for line in sys.stdin:
        user, coach, joined_on = line.rstrip('\n').split('\t')
        if coach != current_coach:
            current_coach = coach
            num_students = 0

        num_students = num_students + 1
        if num_students == 10:
            fill_value(current_coach, joined_on, end_date)


def reduce_students(end_date):
    """Count each student only once. Take the first date
    she had a teacher on and backfill it until required reporting date.
    Relies on the properties of the data fed into it by query, such as:
    - only users with teachers will be included
    - date of first result is a first date on which user
        can be classified as a student
    """

    current_user, dt = sys.stdin.readline().rstrip('\n').split('\t')
    fill_value(current_user, dt, end_date)

    for line in sys.stdin:
        user, dt = line.rstrip('\n').split('\t')
        if user != current_user:
            current_user = user
            fill_value(current_user, dt, end_date)


def main():
    usage_str = "Usage: coach_reduce.py <student|teacher> <end_date>"
    if len(sys.argv) != 3:
        print >> sys.stderr, usage_str
        print >> sys.stderr, "Please specify all arguments"
        exit(1)

    end_date = sys.argv[2]
    if sys.argv[1] == "teacher":
        reduce_coaches(end_date)
    elif sys.argv[1] == "student":
        reduce_students(end_date)
    else:
        print >> sys.stderr, "Unkown option"
        print >> sys.stderr, usage_str
        exit(1)


if __name__ == "__main__":
        main()
