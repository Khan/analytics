#!/usr/bin/python
"""
Map reduce transforms to compute values of student and teachers
"""

import sys
import datetime
import time

date_format = '%Y-%m-%d'


def daterange(start_date, end_date):
    """Date range iterator Yields consecutive dates
        between start_date inclusive and end_date exclusive
    """
    for n in range(int((end_date - start_date).days)):
        yield start_date + datetime.timedelta(n)


def emit_data_row(value, date):
    """Create data row. Friendly wrapper"""

    emit_str = "{0}\t{1}".format("\t".join(value), date)
    print(emit_str)


def fill_value(value, from_dt, to_dt):
    """Replicate given value from from_dt inclusive to to_dt exclusive"""

    from_date = datetime.datetime.strptime(from_dt, date_format).date()
    to_date = datetime.datetime.strptime(to_dt, date_format).date()

    for single_date in daterange(from_date, to_date):
        date_string = time.strftime(date_format, single_date.timetuple())
        emit_data_row(value, date_string)


def reduce_coaches():
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
            emit_data_row([current_coach], joined_on)


def fill_counts(end_date):
    """From sorted rows with values computes sum of previous
        days for each day.
    Fills missing dates with preceeding values"""

    current_count, current_dt = sys.stdin.readline().rstrip('\n').split('\t')
    current_count = int(current_count)

    for line in sys.stdin:
        days_count, dt = line.rstrip('\n').split('\t')
        fill_value([str(current_count)], current_dt, dt)
        current_dt = dt
        current_count = current_count + int(days_count)

    fill_value([str(current_count)], current_dt, end_date)


def active_students(end_date):
    """Compute amount of active students.
    Active student is a user who performed an action
    as defined in user_daily_activity in last 28 days.

    We have to keep record of last 28 days of user activities and when
    computing number of active students for a given day make sure to count
    each student only once.
    """

    student, current_dt = sys.stdin.readline().rstrip('\n').split('\t')
    current_dt_obj = datetime.datetime.strptime(
        current_dt, date_format).date()
    last_28days = [{student: True}]
    last_28dates = [current_dt_obj]

    def get_active_students(dt):
        """For given day compute number of active students.
        Checks all dates for which there have been active
            students in past 28 days"""

        active_students = {}
        for some_time in daterange(dt + datetime.timedelta(-28), dt):
            if some_time in last_28dates:
                idx = last_28dates.index(some_time)
                active_students = dict(list(active_students.items()) +
                    list(last_28days[idx].items()))
        emit_data_row([str(len(active_students))],
            time.strftime(date_format, dt.timetuple()))

    for line in sys.stdin:
        student, dt = line.rstrip('\n').split('\t')
        dt_obj = datetime.datetime.strptime(dt, date_format).date()

        if dt_obj != current_dt_obj:
            # Dates might not be consecutive, although they are sorted
            [get_active_students(missing_date) for
                missing_date in daterange(current_dt_obj, dt_obj)]

            # Add placeholder for values for new date and
            #   remove oldest if there are more than 28
            current_dt_obj = dt_obj
            last_28dates.append(current_dt_obj)
            last_28dates = last_28dates[-28:]
            last_28days.append({})
            last_28days = last_28days[-28:]
        last_28days[len(last_28days) - 1][student] = True

    # Fix for last date for which loop above will not do anything
    end_date_obj = datetime.datetime.strptime(end_date, date_format).date()
    [get_active_students(missing_date) for
        missing_date in daterange(current_dt_obj, end_date_obj)]


def active_teachers(end_date):
    """Compute number of active teachers.
    Active teacher is a teacher with at least 10 active students.

    Code is quite similiar to the one of active_student. However, it might
    be better to keep it separate in case definition changes."""

    last_28days = {}
    last_28dates = {}
    student, current_teacher, current_dt = sys.stdin.readline().rstrip(
        '\n').split('\t')
    current_dt_obj = datetime.datetime.strptime(
        current_dt, date_format).date()
    last_28dates[current_teacher] = [current_dt_obj]
    last_28days[current_teacher] = [{student: True}]

    def get_active_teachers(dt):
        """For given date find all active teachers.
        Takes into account past 28 days"""

        active_teachers = 0
        for some_teacher in last_28days:
            active_students = {}
            for some_time in daterange(dt + datetime.timedelta(-28), dt):
                if some_time in last_28dates[some_teacher]:
                    idx = last_28dates[some_teacher].index(some_time)
                    active_students = dict(list(active_students.items()) +
                        list(last_28days[some_teacher][idx].items()))
            if len(active_students) > 9:
                active_teachers = active_teachers + 1

        emit_data_row([str(active_teachers)],
            time.strftime(date_format, dt.timetuple()))

    for line in sys.stdin:
        student, teacher, dt = line.rstrip('\n').split('\t')
        dt_obj = datetime.datetime.strptime(dt, date_format).date()

        if dt_obj != current_dt_obj:
            [get_active_teachers(missing_date) for
                missing_date in daterange(current_dt_obj, dt_obj)]

            # Add placeholder for new date and remove oldest date
            #   if there are more than 28
            current_dt_obj = dt_obj
            for some_teacher in last_28days:
                last_28days[some_teacher].append({})
                last_28days[some_teacher] = last_28days[some_teacher][-28:]
                last_28dates[some_teacher].append(current_dt_obj)
                last_28dates[some_teacher] = last_28dates[some_teacher][-28:]

        # Add placeholder for new teacher
        if current_teacher != teacher:
            current_teacher = teacher
            last_28days[current_teacher] = [{}]
            last_28dates[current_teacher] = [current_dt_obj]

        last_idx = len(last_28days[current_teacher]) - 1
        last_28days[current_teacher][last_idx][student] = True

    # Fix for last date for which the loop above will not do anything
    end_date_obj = datetime.datetime.strptime(end_date, date_format).date()
    [get_active_teachers(missing_date) for
        missing_date in daterange(current_dt_obj, end_date_obj)]


def main():
    usage_parts = ["Usage: coach_reduce.py <count_func> <end_date>",
        "       coach_reduce.py teacher",
        "count_func can be one of {teacher|count|active-student|active_teacher}"]
    usage_str = "\n".join(usage_parts)
    argc = len(sys.argv)
    if argc < 2:
        print >> sys.stderr, usage_str
        exit(1)

    if sys.argv[1] != "teacher" and argc != 3:
        print >> sys.stderr, usage_str
        exit(1)

    if sys.argv[1] == "teacher":
        reduce_coaches()
    elif sys.argv[1] == "count":
        fill_counts(sys.argv[2])
    elif sys.argv[1] == "active-student":
        active_students(sys.argv[2])
    elif sys.argv[1] == "active-teacher":
        active_teachers(sys.argv[2])
    else:
        print >> sys.stderr, "Unkown option"
        print >> sys.stderr, usage_str
        print >> sys.stderr, "       coach_reduce.py teacher"
        exit(1)


if __name__ == "__main__":
    main()
