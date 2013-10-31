"""One-off analysis of results in experiments to study learning gains.

Overview: this script reads from stdin a data set of "topic_attempts" that
are annotated with what alternative that user was in. It constructs a
"learning curve" per (topic, alternative).  To generate the needed data that
is input to this script, the following steps provide a working example.

Steps to generate input data:
1) user user_experiment_info.q to identity the participants in the experiment
   and their alternatives.  Here's an example command:

   elastic-mapreduce --create --alive --name "topic mode difficulty data" \
      --hive-versions 0.11.0 \
      --num-instances 8 \
      --master-instance-type m2.xlarge \
      --slave-instance-type m2.xlarge \
      --hive-script --arg s3://ka-mapreduce/code/hive/user_experiment_info.q \
      --log-uri s3://ka-mapreduce/logs \
      --args -i,s3://ka-mapreduce/code/hive/ka_hive_init.q \
      --args -d,EXPERIMENT=Topic mode: difficulty \
      --args -d,EXP_PARTITION=topic-mode-difficulty \
      --args -d,dt=2013-06-09

   Note: I had some problems with this job failing because of the spaces in
   the value for EXPERIMENT, so I ended up running the hive code in
   user_experiment_info.q directly in an interactive hive cluster just to be
   safe. Also make sure that dt is set to a recent date lest the partition
   fail to be created.

2) Join the data from step 1 with topic_attempts like follows:
INSERT OVERWRITE DIRECTORY 's3://ka-mapreduce/temp/sitan/learning_analysis'
SELECT t.*, u.alternative
FROM topic_attempts t
INNER JOIN (
    SELECT *
    FROM user_experiment_info
    WHERE experiment="topic-mode-difficulty") u
ON t.user = u.user_id  -- topic_attempts.user should be named user_id
WHERE
  t.dt>='2012-04-10' AND t.dt <= '2013-06-27' AND
  t.topic != 'any';

  Note: Again be sure to tweak the range of acceptable dt's as needed.

3) Download the data from the output directory of step 2, sort it by
   user-topic, and pipe it to this script.  E.g.,

  cd ~/temp
  s3cmd get --recursive s3://ka-mapreduce/temp/sitan/learning_analysis
  cd learning_analysis
  cat -v 000* | sed "s/\^A/,/g" > learning_data.csv
  time sort -s -t, -k1,2 --temporary-directory sorttemp \
    --output=learning_data.sorted learning_data.csv &> sort.log &
  cat learning_data.sorted | python learning_analysis.py [options]

4) Open up the ipython notebook called Learning in the Early Proficiency
   Experiment.ipynb in this directory to do any high-level analysis/plotting.

NOTE: khan/webapp should be in the path

"""

from collections import defaultdict
import optparse
import json
import numpy as np
import sys

from assessment import engine
from assessment import mirt_engine


def load_exercise_difficulties(option, opt_str, value, parser):
    """Optionally load exercise difficulties from external file"""
    global exercise_difficulties
    if value:
        with open(value) as f:
            for line in f:
                attempt = Attempt(line)
                # check if test card
                if attempt.scheduler_info.get('purpose', None) == 'randomized':
                    ex_diff = exercise_difficulties[attempt.exercise]
                    ex_diff.num_analytics_cards += 1
                    ex_diff.num_correct += attempt.correct
        print "Difficulties calibrated for %d exercises." % (
                len(exercise_difficulties.keys()))


def get_cmd_line_options():
    parser = optparse.OptionParser()
    parser.add_option("-m", "--min_attempts", type=int,
                      help=("Ignore users who have made fewer than "
                            "min_attempts attempts."))
    parser.add_option("-d", "--difficulty_cutoff", type=int,
                      help=("Ignore exercises above a certain difficulty."))
    parser.add_option("-t", "--truncate", type=int,
                      help=("Ignore last few questions in case of bailout."))
    parser.add_option("-l", "--load_diffs", action="callback", type="string",
                      callback=load_exercise_difficulties,
                      help=("Load exercise difficulties from external file."))
    parser.add_option("-e", "--mirt_model", type=str, default="mirt_math.npz",
                      help=("Model to be provided to MIRT engine"))
    parser.add_option("-o", "--output", type=str, default="results.npz",
                      help=("Filename of output of analysis."))
    options, args = parser.parse_args()
    return options, args


class LearningCurvePoint:
    def __init__(self):
        self.num_analytics_cards = 0
        self.time_taken = 0
        self.num_correct = 0
        self.raw_incremental_gain = 0.0  # correctness gain
        self.norm_incremental_gain = 0.0  # abilities gain
        self.total_increments = 0.0
        self.sum_difficulty = 0.0
        self.sum_abilities = 0.0  # sum of means of abilities samples each step
        self.sum_abilities_weighted = 0.0  # sum of means divided by variance
        self.sum_abilities_weights = 0.0  # sum of reciprocals of variances
        self.num_all_cards = 0  # of cards, both assessment and non-assessment


class Attempt:
    def __init__(self, line):
        (user, topic, exercise, time_done, time_taken,
                problem_number, correct, scheduler_info, user_segment, dt,
                alternative) = line.rstrip().split(',')
        self.user = user
        self.topic = topic
        self.exercise = exercise
        self.time_done = float(time_done)
        # NOTE: capping time_taken at 10 minutes to remove outliers
        self.time_taken = min(int(time_taken), 600)
        self.problem_number = int(problem_number)
        self.correct = correct == "true"
        self.scheduler_info = json.loads(scheduler_info)
        self.user_segment = user_segment
        self.dt = dt
        self.alternative = alternative

options, args = get_cmd_line_options()

topic_curves = defaultdict(lambda:
        defaultdict(lambda: defaultdict(LearningCurvePoint)))

# each exercise should have a difficulty, measured by percentage correct
exercise_difficulties = defaultdict(LearningCurvePoint)

# keys are (exercise, correct) tuples,
# values are 1d ndarrays of 1d MIRT abilities
exercise_correct_abilities = {}

mirt_model = mirt_engine.MIRTEngine(options.mirt_model)


def exercise_difficulty(exercise_name):
    """Simple lookup of the bias coupling for exercise_name."""
    if exercise_name not in mirt_model.exercise_ind_dict:
        return None
    else:
        index = mirt_model.exercise_ind_dict[exercise_name]
        return mirt_model.couplings[index, 0]


def posterior_ability_samples(exercise_name, correct, num_samples, burn_in=20):
    """Given an exercise name and whether it was answered correctly,
    compute an estimate of the user's ability based on that one data point.
    """
    if exercise_name not in mirt_model.exercise_ind_dict:
        return None

    response = engine.ItemResponse.new(correct, exercise_name,
            None, None, None, None, None, None, None, None, None)
    history = [response.data]

    # burn-in
    mirt_model._update_abilities(history, use_mean=True, num_steps=burn_in)

    # create sample chain, note that this only makes sense for 1d MIRT
    # TODO(sitan): move to mirt_util or mirt_engine in webapp
    abilities_samples = np.zeros(num_samples)
    for step in range(num_samples):
        mirt_model._update_abilities(history, use_mean=False, num_steps=1)
        abilities_samples[step] = mirt_model.abilities[0, 0]

    return abilities_samples


def abilities_estimate_for_response(exercise_name, correct):
    """Retrieves ability estimate from exercise_correct_abilities if known.
    If not, updates exercise_correct_abilities accordingly.
    """
    key = (exercise_name, correct)

    if key not in exercise_correct_abilities:
        exercise_correct_abilities[key] = posterior_ability_samples(
                exercise_name, correct, 1000, 200)

    return exercise_correct_abilities[key]


def parse_input(callback, options):
    """Parse through lines of stdin"""
    lines_processed = 0
    stats = {
        'lines_kept': 0,
        'lines_tossed': 0,
    }

    def should_skip(attempts):
        """If there are any exercises that we do not know the first problem
        for, then skip-- we don't have complete information, and therefore
        the card number in the topic is unknown.
        """
        exercises_seen = []
        for attempt in attempts:
            if attempt.exercise not in exercises_seen:
                if attempt.problem_number != 1:
                    stats['lines_tossed'] += len(attempts)
                    return True
                exercises_seen.append(attempt.exercise)
        #TODO(jace): should also check that every problem_number goes up by +1
        stats['lines_kept'] += len(attempts)
        return False

    prev_user_topic = None
    prev_alternative = None
    attempts = []
    user_alternative = None
    bad_user = False

    def process_user_topic(attempts):
        attempts.sort(key=lambda attempt: attempt.time_done)
        if not should_skip(attempts) and not bad_user:
            callback(attempts, prev_user_topic[0], prev_user_topic[1],
                     prev_alternative, options)

    for line in sys.stdin:
        attempt = Attempt(line)

        lines_processed += 1
        if lines_processed % 1e6 == 0:
            print >>sys.stderr, "Processed %d lines." % lines_processed

        user_topic = (attempt.user, attempt.topic)

        # Every time we see a new (user, topic), first process and flush the
        # buffered data for the previous (user, topic).
        if user_topic != prev_user_topic and prev_user_topic:
            process_user_topic(attempts)
            attempts = []

        # TODO(jace): it seems some user_id's are present in more than
        # one alternative in the GAEBingoIdentiyRecord.  The right solution
        # would be to fix the root cause, and barring that, to throw away
        # all data for a user that ultimately appears in more than one
        # alternative.  For right now, I skip some of such user's data,
        # but only AFTER we see the inconsistent alternatives.
        if (not prev_user_topic) or (prev_user_topic[0] != attempt.user):
            user_alternative = attempt.alternative
            bad_user = False
        if attempt.alternative != user_alternative and not bad_user:
            print >>sys.stderr, (
                "A user is in multiple alternatives. %s,%s,%s"
                % (str(user_topic), attempt.alternative, user_alternative))
            bad_user = True

        attempts.append(attempt)
        prev_user_topic = user_topic
        prev_alternative = attempt.alternative

    # process last new (user,topic)
    process_user_topic(attempts)

    print "lines kept: %d " % stats['lines_kept']
    print "lines tossed: %d " % stats['lines_tossed']


def update_topic_curves(attempts, user, topic, alternative, options):
    # Option 1: ignore users who have made too few attempts
    if options.min_attempts:
        if len(attempts) < options.min_attempts:
            return

    is_test = lambda info: info.get('purpose', None) == 'randomized'
    # a test_card is a tuple of (attempt_index, corrrect, time_taken)
    test_cards = [(i, a) for i, a in enumerate(attempts)
                  if is_test(a.scheduler_info)]

    # first, increment count of all cards-- both assessment and regular
    for i in range(len(attempts)):
        topic_curves[alternative][topic][i].num_all_cards += 1

    # further filter test_cards to only keep ones with exercises known by
    # our MIRT model...
    known_exs = mirt_model.exercise_ind_dict.keys()
    test_cards = [(i, a) for (i, a) in test_cards
                  if a.exercise in known_exs]

    # Option 2: filter by exercises above or below a given difficulty
    if options.difficulty_cutoff:
        def keep_difficulty(ex):
            ind = mirt_model.exercise_ind_dict[ex]
            #print ex, mirt_model.couplings[ind, -1]
            return mirt_model.couplings[ind, -1] < options.difficulty_cutoff
        test_cards = [(i, a) for (i, a) in test_cards
                      if keep_difficulty(a.exercise)]

    # Option 3: trim off the last 'truncate' test_cards,
    # in case there is a bailout effect
    if options.truncate:
        if len(test_cards) > options.truncate:
            test_cards = test_cards[:-options.truncate]
        else:
            test_cards = []

    for i in range(len(test_cards)):
        attempt_index, attempt = test_cards[i]
        curr_ex = attempt.exercise

        # retrieve ability estimate chain
        abilities_samples = abilities_estimate_for_response(
                curr_ex, attempt.correct)

        # first update the naive learning curve at the position for this
        # assessment card
        curve_point = topic_curves[alternative][topic][attempt_index]
        curve_point.num_analytics_cards += 1
        curve_point.time_taken += attempt.time_taken
        curve_point.num_correct += attempt.correct
        curve_point.sum_difficulty += exercise_difficulty(curr_ex)

        mean = np.mean(abilities_samples)
        var = np.var(abilities_samples)
        curve_point.sum_abilities += mean
        curve_point.sum_abilities_weighted += mean / var
        curve_point.sum_abilities_weights += 1.0 / var

    # this is an alternative, "incremental" analysis where we explicitly
    # use the difference in estimated abilities between two assessment
    # cards to represent the learning gain.
    for i in range(1, len(test_cards)):
        prev_card, curr_card = test_cards[i - 1], test_cards[i]
        prev_index, prev_attempt = prev_card
        curr_index, curr_attempt = curr_card
        curr_ex, curr_crct = curr_attempt.exercise, curr_attempt.correct
        prev_ex, prev_crct = prev_attempt.exercise, prev_attempt.correct

        total_gain = float(curr_crct) - float(prev_crct)

        # One way to measure incremental gain is the difference of the
        # correctness values divided by the distance (# of cards) between
        # assessment cards
        increment_size = 1.0 / (curr_index - prev_index)
        raw_incremental_gain = increment_size * total_gain

        # Another way is to use the MIRT model to estimate abilities from
        # each card
        curr_abilities = abilities_estimate_for_response(curr_ex, curr_crct)
        curr_abilities = np.mean(curr_abilities)
        prev_abilities = abilities_estimate_for_response(prev_ex, prev_crct)
        prev_abilities = np.mean(prev_abilities)
        norm_incremental_gain = increment_size * (
            curr_abilities - prev_abilities)

        # second, add the incremental gain to each of the positions
        # spanning the last two assessment cards
        for j in range(prev_index, curr_index):
            curve_point = topic_curves[alternative][topic][j]
            curve_point.raw_incremental_gain += raw_incremental_gain
            curve_point.norm_incremental_gain += norm_incremental_gain
            curve_point.total_increments += increment_size


def print_curve(curve, topic, alternative):
    print "CURVE: topic=%s, alt=%s" % (topic, alternative)
    lines = [""] * 9
    for c in range(60):
        if c in curve and curve[c].num_analytics_cards:
            lines[0] += "%d, " % c
            lines[1] += "%d, " % curve[c].num_all_cards
            lines[2] += "%d, " % curve[c].num_analytics_cards
            lines[3] += "%d, " % curve[c].num_correct
            lines[4] += "%.3f, " % (float(curve[c].num_correct) /
                                    float(curve[c].num_analytics_cards))
            lines[5] += "%.3f, " % (float(curve[c].sum_abilities) /
                                    float(curve[c].num_analytics_cards))
            lines[6] += "%.3f, " % (float(curve[c].sum_abilities_weighted) /
                                    float(curve[c].sum_abilities_weights))
            lines[7] += "%.3f, " % (float(curve[c].sum_difficulty) /
                                    float(curve[c].num_analytics_cards))
            lines[8] += "%d, " % curve[c].time_taken
    print "\n".join(lines)


def aggregate_curve(from_curve, to_curve):
    # add contents of from_curve to aggregate to_curve
    for c in range(100):
        if c not in from_curve:
            continue
        to_curve[c].num_analytics_cards += from_curve[c].num_analytics_cards
        to_curve[c].num_correct += from_curve[c].num_correct
        to_curve[c].raw_incremental_gain += from_curve[c].raw_incremental_gain
        to_curve[c].sum_abilities += from_curve[c].sum_abilities
        # TODO(sitan): not sure if this is the canonical way to split
        to_curve[c].sum_abilities_weighted += (
                from_curve[c].sum_abilities_weighted)
        to_curve[c].sum_abilities_weights += (
                from_curve[c].sum_abilities_weights)
        to_curve[c].sum_difficulty += from_curve[c].sum_difficulty
        to_curve[c].time_taken += from_curve[c].time_taken
        to_curve[c].num_all_cards += from_curve[c].num_all_cards


def write_npz_file(agg_curve):
    # first convert all defaultdicts to dicts
    global topic_curves
    topic_curves = dict(topic_curves)
    for alt in topic_curves.keys():
        agg_curve[alt] = dict(agg_curve[alt])
        topic_curves[alt] = dict(topic_curves[alt])
        for topic in topic_curves[alt].keys():
            topic_curves[alt][topic] = dict(topic_curves[alt][topic])
    np.savez(options.output, topic_curves=topic_curves, agg_curve=agg_curve)

if __name__ == '__main__':
    parse_input(update_topic_curves, options)
    alternatives = topic_curves.keys()
    # print topic results as well as an aggregation
    agg_curve = {}
    for alternative in alternatives:
        agg_curve[alternative] = defaultdict(LearningCurvePoint)

    for topic in topic_curves[alternatives[0]].keys():
        for i in range(1, len(alternatives)):
            if topic not in topic_curves[alternatives[i]]:
                print "Weird.. Topic %s found in only one alternative." % topic
                continue
        for alternative in alternatives:
            print_curve(topic_curves[alternative][topic], topic, alternative)
            # add this topic's data to the aggregate curve
            aggregate_curve(topic_curves[alternative][topic],
                            agg_curve[alternative])

    for alternative in alternatives:
        print_curve(agg_curve[alternative], "AGGREGATE", alternative)

    write_npz_file(agg_curve)
