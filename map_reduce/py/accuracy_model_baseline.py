"""TODO(jace):  REMOVE ME!  This is modified version of the production
accuracy model.  It's hacked up to to include the parameters inline
instead of importing them.  Someday soon we should jar up website/stable
and make it importable from the EMR/Hive environment.
"""


class Params:
    pass
params = Params()
params.INTERCEPT = -1.2229719
params.EWMA_3 = 0.8393673
params.EWMA_10 = 2.1262489
params.CURRENT_STREAK = 0.0153545
params.LOG_NUM_DONE = 0.4135883
params.LOG_NUM_MISSED = -0.5677724
params.PERCENT_CORRECT = 0.6284309

#==========START REGULAR FILE, MINUS THE PARAM IMPORTING============

import itertools
import math
import operator


# Instead of importing these params, I specify them inline.
# TODO(jace): One day, we'll be able to import anything from Website/stable.
# TODO(david): Find out what this actually is
PROBABILITY_FIRST_PROBLEM_CORRECT = 0.8

# Seeded on the mean correct of a sample of 1 million problem logs
# TODO(david): Allow these seeds to be adjusted or passed in, or at
#     least use a more accurate seed (one that corresponds to P(first
#     problem correct)).
EWMA_SEED = 0.9

# We only look at a sliding window of the past problems. This is to minimize
# space requirements as well as allow the user to recover faster.
MAX_HISTORY_KEPT = 20
MAX_HISTORY_BIT_MASK = (1 << MAX_HISTORY_KEPT) - 1


def bit_count(num):
    # TODO(david): This uses Kerninghan's method, which would not be very quick
    #     for dense 1s. Use numpy or some library.
    count = 0
    while num:
        num &= num - 1
        count += 1
    return count


class AccuracyModel(object):
    """
    Predicts the probabilty of the next problem correct using logistic
    regression.
    """

    # Bump this whenever you change the state we keep around so we can
    # reconstitute existing old AccuracyModel objects. Also remember to update
    # the function update_to_new_version accordingly.
    CURRENT_VERSION = 1

    def __init__(self, user_exercise=None):
        self.version = AccuracyModel.CURRENT_VERSION

        # A bit vector for keeping up to the last 32 problems done
        self.answer_history = 0

        # This is capped at MAX_HISTORY_KEPT
        self.total_done = 0

        if user_exercise is not None:
            # Switching the user from streak model to new accuracy
            # model. Use current streak as known history, and simulate
            # streak correct answers.
            self.update([True] * user_exercise.streak)

    def update(self, correct):
        if self.version != AccuracyModel.CURRENT_VERSION:
            self.update_to_new_version()

        if hasattr(correct, '__iter__'):
            # TODO(david): This can definitely be made more efficient.
            for answer in correct:
                self.update(answer or answer == '1')
        else:
            self.total_done = min(self.total_done + 1, MAX_HISTORY_KEPT)
            self.answer_history = \
                ((self.answer_history << 1) | correct) & MAX_HISTORY_BIT_MASK

        return self

    def update_to_new_version(self):
        """
        Updates old AccuracyModel objects to new objects. This function should
        be updated whenever we make a change to the internal state of this
        class.
        """

        # Bump this up when bumping up CURRENT_VERSION. This is here to ensure
        # that this function gets updated along with CURRENT_VERSION.
        UPDATE_TO_VERSION = 1
        assert UPDATE_TO_VERSION == AccuracyModel.CURRENT_VERSION

    # 0-based index where 0 is the most recent problem done
    def get_answer_at(self, index):
        return (self.answer_history >> index) & 1

    # http://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average
    def exp_moving_avg(self, weight):
        ewma = EWMA_SEED

        for i in reversed(xrange(self.total_done)):
            ewma = weight * self.get_answer_at(i) + (1 - weight) * ewma

        return ewma

    def streak(self):
        for i in xrange(self.total_done):
            if not self.get_answer_at(i):
                return i

        return self.total_done

    def total_correct(self):
        mask = (1 << self.total_done) - 1
        return bit_count(self.answer_history & mask)

    def predict(self):
        """
        Returns: the probabilty of the next problem correct using
        logistic regression.
        """

        if self.version != AccuracyModel.CURRENT_VERSION:
            self.update_to_new_version()

        # We don't try to predict the first problem (no user-exercise history)
        if self.total_done == 0:
            return PROBABILITY_FIRST_PROBLEM_CORRECT

        # Get values for the feature vector X
        ewma_3 = self.exp_moving_avg(0.333)
        ewma_10 = self.exp_moving_avg(0.1)
        current_streak = self.streak()
        log_num_done = math.log(self.total_done)
        # log (num_missed + 1)
        log_num_missed = math.log(self.total_done - self.total_correct() + 1)
        percent_correct = float(self.total_correct()) / self.total_done

        weighted_features = [
            (ewma_3, params.EWMA_3),
            (ewma_10, params.EWMA_10),
            (current_streak, params.CURRENT_STREAK),
            (log_num_done, params.LOG_NUM_DONE),
            (log_num_missed, params.LOG_NUM_MISSED),
            (percent_correct, params.PERCENT_CORRECT),
        ]

        X, weight_vector = zip(*weighted_features)  # unzip the list of pairs

        return AccuracyModel.logistic_regression_predict(
            params.INTERCEPT, weight_vector, X)

    def is_struggling(self, param, minimum_accuracy, minimum_attempts):
        """ Whether or not this model detects that the student is struggling
        based on the history of answers thus far.

        param - This is an exponent which measures how fast we expect students
        to achieve proficiency and get out. The larger the number, the longer
        we allow them to experiment. This is only injected for experimentation
        purposes - it will be internalized later.

        minimum_accuracy - minimum accuracy required for proficiency

        minimum_attempts - minimum problems done before making a judgement
        """

        attempts = self.total_done
        if attempts < minimum_attempts:
            return False

        accuracy_prediction = self.predict()
        if accuracy_prediction >= minimum_accuracy:
            return False

        value = (attempts ** param) * (minimum_accuracy - accuracy_prediction)
        return value > 20.0

    # See http://en.wikipedia.org/wiki/Logistic_regression
    @staticmethod
    def logistic_regression_predict(intercept, weight_vector, X):
        # TODO(david): Use numpy's dot product fn when we support numpy
        dot_product = sum(itertools.imap(operator.mul, weight_vector, X))
        z = dot_product + intercept

        return 1.0 / (1.0 + math.exp(-z))

    @staticmethod
    def simulate(answer_history):
        model = AccuracyModel()
        model.update(answer_history)
        return model.predict()

    # The minimum number of problems correct in a row to be greater
    # than the given threshold
    @staticmethod
    def min_streak_till_threshold(threshold):
        model = AccuracyModel()

        for i in itertools.count(1):
            model.update(correct=True)

            if model.predict() >= threshold:
                return i
