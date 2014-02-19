import re

# use a field delimeter that will work in or outside of Hive
linesplit = re.compile('[,\t\x01]')


class FieldIndexer:
    def __init__(self, field_names):
        for i, field in enumerate(field_names):
            self.__dict__[field] = i

    topic_attempt_fields = ['user', 'topic', 'exercise', 'time_done',
            'time_taken', 'problem_number', 'correct', 'scheduler_info',
            'user_segment', 'dt']

    plog_fields = ['user', 'time_done', 'exercise', 'problem_type',
            'seed', 'time_taken', 'problem_number', 'correct',
            'number_attempts', 'number_hints', 'eventually_correct',
            'review_mode', 'task_type', 'skipped', 'dt']


def sequential_problem_numbers(attempts, idx):
    """Takes all problem logs for a user as a list of lists, indexed by idx,
    and makes sure that problem numbers within an exercise are strictly
    increasing and never jump by more than one.
    """
    ex_prob_number = {}  # stores the current problem number for each exercise
    for attempt in attempts:

        ex = attempt[idx.exercise]
        prob_num = attempt[idx.problem_number]

        if ex not in ex_prob_number:
            ex_prob_number[ex] = prob_num
        else:
            if prob_num == ex_prob_number[ex] + 1:
                ex_prob_number[ex] = prob_num
            else:
                #print "Bad line is:"
                #print attempt
                return False
    return True


def incomplete_history(attempts, idx):
    """Takes all problem logs for a user as a list of lists.  The inner lists
    each represent a problem attempt, with items described and indexed by the
    idx argument.  This function returns True if we *know* we have an
    incomplete history for the user, by checking if the first problem seen
    for any exercise has a problem_number != 1.
    """
    exercises_seen = []
    for attempt in attempts:
        if attempt[idx.exercise] not in exercises_seen:
            if int(attempt[idx.problem_number]) != 1:
                return True
            exercises_seen.append(attempt[idx.exercise])
    return False


def valid_history(attempts, idx):
    if not sequential_problem_numbers(attempts, idx):
        return False

    if incomplete_history(attempts, idx):
        return False

    return True
