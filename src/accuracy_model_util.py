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

    plog_fields = ['user', 'time_done', 'rowtype', 'exercise', 'problem_type',
            'seed', 'time_taken', 'problem_number', 'correct',
            'number_attempts', 'number_hints', 'eventually_correct',
            'topic_mode', 'dt']


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


def valid_history(attempts, idx):
    if not sequential_problem_numbers(attempts, idx):
        #print >>sys.stderr, "Invalid History: Non-sequential problem numbers."
        return False

    return True
