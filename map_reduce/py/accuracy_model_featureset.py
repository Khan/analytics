#!/usr/bin/env python
"""
This script generates a data set of features which can be passed to a
classifier training program to build an accuracy model. The following steps
document example usage of this script in conjunction with
accuracy_model_train.py. I recommend you work through this script end-to-end
with a tiny data set, to smooth things out, and then start over once things
are fixed with a large data.

You may also find this document useful: https://docs.google.com/document/d/1POHUcj-b_AtwqMEIdiaH2TP84nFBuEfQwhvZPt5gl1Y/edit?usp=sharing

---

Step 1) Collect raw data from the ProblemLog table.
===============================================================================
Be sure to include all columns that may be useful for generating features to be
fed into a statistical model, later.

We also want to make sure that we include users with plenty of experience over
many exercises and a decent time window.

The final data must also be sorted by user, then by exercise, then by
problem_number so that accuracy_model_featureset.py works correctly.

The easiest way to do this is by executing these series of queries in BigQuery:

1a) Find users that have been active on a large number of exercises for the
time window you are interested in.  This query found ~250,000 users.

SELECT user.email, count(*) as exercise_count
FROM [2014_01_22.UserExercise]
WHERE total_done >= 5 AND last_done > timestamp('2013-08-01')
GROUP BY user.email
HAVING COUNT(*) > 70

Save the results of this query into a table, recent_diversely_active_users.

1b) Extract the relevant problem logs for the users and time window you are
interested in.  This query only exports the first 5 problems for each exercise
for each user, since I knew that was the type of data I wanted to train the
model over.

SELECT pl.user.email,
  time_done,
  exercise,
  problem_type,
  seed,
  time_taken,
  problem_number,
  correct,
  count_attempts,
  count_hints,
  -- TODO(mattfaus): This column is no longer necessary and will only ever be
  -- populated with false. This is because problemlogs with count_attemps > 1
  -- are always marked as incorrect.
  (count_hints = 0 AND count_attempts > 1 AND correct) as eventually_correct,
  review_mode,
  task_type,
  skipped
FROM [ben5.ProblemLog_2013_12_22] pl
JOIN EACH [mattfaus.recent_diversely_active_users] au
ON pl.user.email = au.user_email
WHERE problem_number <= 5
and time_done >= timestamp('2013-08-01')
and au.exercise_count > 70;
-- TODO(mattfaus): When BigQuery supports it, add this ordering, so you can
-- skip step 1e below. Right now it gives a response too large to output error.
-- ORDER BY pl.user.email, exercise, problem_number

Save the results of this query into a table, like km_training_data.

1c) Export the km_training_data as a set of CSV files into Google Cloud
Storage, by clicking the appropriate buttons in the BigQuery UI.

1d) Download the CSV files from GCS onto Aristotle, or whatever beefy machine
you inteend to use to train the model. I launched 4 concurrent downloads like
this:

nohup gsutil cp gs://mattfaus/km_training_data3/000* . &
nohup gsutil cp gs://mattfaus/km_training_data3/001* . &
nohup gsutil cp gs://mattfaus/km_training_data3/002* . &
nohup gsutil cp gs://mattfaus/km_training_data3/003* . &

1e) Cleanup the CSVs, this includes:
- Removing the header line from each one.

for files in `ls 0*`
do
  tail -n +2 $files > $files.csv
done

- Combine and sort them by user, exercise, and then problem_number

sort -s -t, -k1,1 -k3,3 -k7g,7 *.csv > problemlogs.csv

---------------
Or, you can try this query in Hive:

set hivevar:start_dt=2013-10-01;
set hivevar:end_dt=2014-01-10;

INSERT OVERWRITE DIRECTORY 's3://ka-mapreduce/temp/mattfaus/accmodel2'
SELECT
  get_json_object(problemlog.json, '$.user') AS user,
    cast(get_json_object(problemlog.json, '$.time_done') as double)
        AS time_done,
    get_json_object(problemlog.json, '$.exercise') AS exercise,
    get_json_object(problemlog.json, '$.problem_type') AS problem_type,
    get_json_object(problemlog.json, '$.seed') AS seed,
    cast(get_json_object(problemlog.json, '$.time_taken') as int)
        AS time_taken,
    cast(get_json_object(problemlog.json, '$.problem_number') as int)
        AS problem_number,
    get_json_object(problemlog.json, '$.correct') = "true" AS correct,
    get_json_object(problemlog.json, '$.count_attempts') AS number_attempts,
    get_json_object(problemlog.json, '$.count_hints') AS number_hints,
    (get_json_object(problemlog.json, '$.count_hints') = 0 AND
       (get_json_object(problemlog.json, '$.count_attempts') > 1
        OR get_json_object(problemlog.json, '$.correct') = "true"))
      AS eventually_correct,
    get_json_object(problemlog.json, '$.review_mode') AS review_mode,
    get_json_object(problemlog.json, '$.task_type') AS task_type,
    get_json_object(problemlog.json, '$.skipped') AS skipped,
    dt AS dt
FROM problemlog
WHERE
  dt >= '${start_dt}' AND dt < '${end_dt}'
ORDER BY user, exercise, problem_number;

Transfer the raw data from S3 onto the machine which will process it. I
recommend using something beefy.

s3cmd get --recursive s3://ka-mapreduce/temp/mattfaus/accmodel2

Convert the Hive binary format into CSV format.  These sed scripts should
work as-is, but be careful with the arguments to cat, they may need to be
changed depending on the environment in which you are executing.

cat -etv 000* | sed  "s/\^A/,/g" | sed "s/\\\N/NULL/g" > problemlogs.csv


Step 2) Split your data into training and test sets.
===============================================================================
At this point, you should have one gigantic problemlogs.csv file which contains
richly diverse data over a large time window and is sorted correctly. You need
to segment it into some smaller chunks for training and testing the model.

TODO(mattfaus): Use split_test_train_data.py instead to not introduce bias.

When mattfaus did this in 1/2014, he just created datasets of 20M rows each:

$ wc -l problemlogs.csv
68,351,829 problemlogs.csv

head -n 20000000 problemlogs.csv > training.csv
tail -n +20000000 problemlogs.csv | head -n 20000000 > test1.csv
tail -n +40000000 problemlogs.csv | head -n 20000000 > test2.csv
tail -n +60000000 problemlogs.csv | head -n 20000000 > test3.csv


Step 3) Convert the raw data into features.
===============================================================================
Pipe the correctly formatted data into accuracy_model_featureset.py.
An important part of this step is keeping the random components in sync with
what is being used in production. If you plan on productionizing this model,
you must specify both the --rand_comp_input_file and --rand_comp_output_file
parameters.

3a)

cat training.csv \
| python accuracy_model_featureset.py \
--rand_comp_input_file /dev/webapp/exercises/knowledge_params.pickle \
--rand_comp_output_file newRandomComponents.pkl > features.csv

You will probably need to modify your PYTHONPATH considerably to get this to
work, since this script pulls in code from both webapp and different
directories of the analytics repo. Try running this before the command above:

PYTHONPATH=/Users/mattfaus/dev/analytics/map_reduce/py:\
/Users/mattfaus/dev/webapp:\
/Users/mattfaus/dev/webapp/exercises:\
/Users/mattfaus/dev/analytics/src \
; export PYTHONPATH

3b) Sort the samples by exercise.

The previous steps emitted samples in user order, but the
accuracy_model_train.py script requires that the input be sorted by exercise.
Since the exercise ID is the first column in the samples, you can sort the
samples by exercises with this command:

sort -s -t, -k1 features.csv > features.byExercise.csv


Step 4) Train a few models using accuracy_model_train.py.
DATAFILE=/ebs/kadata/accmodel/plog/feat100.1-2.sorted.csv
CODEDIR=/ebs/kadata/accmodel/code
OUTDIR=/home/analytics/tmp/jace/roc
cd $CODEDIR
time cat $DATAFILE | python accuracy_model_train.py \
    --feature_list=baseline --no_bias \
    | grep "rocline" > $OUTDIR/baseline.csv
time cat $DATAFILE | python accuracy_model_train.py \
    --feature_list=none | grep "rocline" > $OUTDIR/bias.csv
time cat $DATAFILE | python accuracy_model_train.py \
    --feature_list=custom -r comps.pickle -o models_custom_only.pickle \
    | grep "rocline" > $OUTDIR/bias+custom.csv
time cat $DATAFILE | python accuracy_model_train.py \
    --feature_list=random -r comps.pickle -o models_random_only.pickle \
    | grep "rocline" > $OUTDIR/bias+random.csv
time cat $DATAFILE | python accuracy_model_train.py \
    --feature_list=custom,random -r comps.pickle -o models.pickle \
    | grep "rocline" > $OUTDIR/bias+custom+random.csv

Step 5)
Use compare_accuracy_models.py to find the best model.

Write a report that looks something like this:
https://docs.google.com/a/khanacademy.org/document/d/1POHUcj-b_AtwqMEIdiaH2TP84nFBuEfQwhvZPt5gl1Y/edit#heading=h.d44n95q6izu8

Step 5a)
Repeat steps 2-5 until you have found a great model.

Step 6)
Checkin the resulting model definition files to webapp, probably under an A/B
test, as described in http://phabricator.khanacademy.org/D6462

"""

import ast
import json
import math
import numpy as np
import optparse
import pickle
import random
import sys

# from webapp; needs to be in PYTHONPATH.
import accuracy_model as model

import accuracy_model_util as acc_util
import random_features

NUM_RANDOM_FEATURES = 100

error_invalid_history = 0
emitted_samples = 0

idx = acc_util.FieldIndexer(acc_util.FieldIndexer.plog_fields)

rand_features = random_features.RandomFeatures(NUM_RANDOM_FEATURES)


def get_baseline_features(ex_state, options):
    """Return a list of feature values from the baseline AccuracyModel."""
    if ex_state.total_done:
        log_num_done = math.log(ex_state.total_done)
        pct_correct = float(ex_state.total_correct()) / ex_state.total_done
    else:
        log_num_done = 0.0  # avoid log(0.)
        pct_correct = model.PROBABILITY_FIRST_PROBLEM_CORRECT

    return [ex_state.exp_moving_avg(0.333),
            ex_state.exp_moving_avg(0.1),
            ex_state.streak(),
            log_num_done,
            math.log(ex_state.total_done - ex_state.total_correct() + 1),
            pct_correct]


def emit_sample(attempt, attempt_number, ex_states, options):
    """Emit a single sample vector based on state prior to this attempt."""
    ex = attempt[idx.exercise]
    outlist = []
    outlist += [attempt[idx.exercise]]
    outlist += ["%d" % attempt[idx.correct]]
    outlist += ["%.4f" % ex_states[ex].predict()]
    outlist += ["%d" % len(ex_states)]
    outlist += ["%d" % attempt[idx.problem_number]]
    outlist += ["%d" % attempt_number]

    # print all the feature values for the existing accuracy model
    for feature in get_baseline_features(ex_states[ex], options):
        outlist += ["%.6f" % feature]

    # print random features
    outlist += ["%.6f" % f for f in rand_features.get_features()]

    global emitted_samples
    emitted_samples += 1
    sys.stdout.write(",".join(outlist) + "\n")


def emit_samples(attempts, options):
    """TODO(jace)"""

    # Make absolutely sure that the attempts are ordered by time_done. This is
    # very important because this is how the user has progressed through the
    # site. problem_number will not work, because that is local to exercise.
    attempts.sort(key=lambda x: x[idx.time_done])

    # If we know we don't have full history for this user, skip her.
    # TODO(jace): restore this check?
    #if acc_util.incomplete_history(attempts, idx):
        #return

    if not acc_util.sequential_problem_numbers(attempts, idx):
        global error_invalid_history
        error_invalid_history += len(attempts)
        return

    # We've passed data validation. Go ahead and process this user.

    # Build a legacy AccuracyModel per-exercise
    ex_states = {}
    rand_features.reset_features()

    # Loop through each attempt, already in proper time order.
    for i, attempt in enumerate(attempts):

        ex = attempt[idx.exercise]
        ex_state = ex_states.setdefault(ex, model.AccuracyModel())

        problem_number = int(attempt[idx.problem_number])

        # *Before* we update state, see if we want to sample
        if options.sampling_mode == 'nth':
            freq = int(options.sampling_param)
            if random.randint(1, freq) == freq:
                emit_sample(attempt, i, ex_states, options)
        elif options.sampling_mode == 'prob_num':
            if problem_number >= options.sampling_param[0] and (
                    problem_number < options.sampling_param[1]):
                emit_sample(attempt, i, ex_states, options)
        elif options.sampling_mode == 'randomized':
            purpose = attempt[idx.scheduler_info].get('purpose', None)
            if purpose == 'randomized':
                emit_sample(attempt, i, ex_states, options)

        # Now that we've written out the sample, update features.
        # First, the baseline features.
        ex_state.update(attempt[idx.correct])

        # Next, the random features.  IMPORTANT: For right now, we only update
        # the random features with the *first* attempt on each exercise. This
        # was done in the hopes that the feature distributions would remain
        # as stable as possible in the context of rolling out the
        # "early proficiency" experiment that was expected to modify the
        # typical number of problems done on exercises.
        if problem_number == 1:
            component = (ex, attempt[idx.problem_type], attempt[idx.correct])
            rand_features.increment_component(component)


def get_cmd_line_options():
    parser = optparse.OptionParser()

    # TODO(jace): convert to argparse. Until then, formatting will be screwed.
    parser.add_option("-s", "--sampling_mode", default="prob_num",
        help="Determines which problem attempts get included in "
             "the data sample.  Three modes are possible:"
             "  randomized - use only the random assessment cards. NOTE: This "
             "      mode is currently broken, since problem log input is "
             "      assumed, and we need topic_mode input to know which cards "
             "      were random. "
             "  nth - use only 1 in every N cards as a sample "
             "  prob_num - output only if problem_number is within the range "
             "      specified by the sampling_param option. sampling_param is "
             "      a string, but should evaluate to a tuple of length 2 "
             "      through ast.literal_eval(). The 2 values are the start "
             "      and end of a range. "
             "      Ex:  '--sampling_mode=prob_num --sampling_param=(1,6)' "
             "      sample problem numbers 1 through 5. ")

    parser.add_option("-p", "--sampling_param", type=str, default="(1,6)",
        help="This parameter is used in conjuction with some samlpling "
             "modes. See documentation sampling_mode for each mode.")

    parser.add_option("-c", "--rand_comp_input_file", default=None,
        help="If provided, will use the random components previously "
            "generated for all exercises. You must use this ability if "
            "you want to keep the existing production KnowledgeState objects.")

    parser.add_option("-r", "--rand_comp_output_file", default=None,
        help="If provided, a file containing the random components will be "
             "output. If a model gets productionized, you need to have this "
             "record of the random component vectors.")

    options, _ = parser.parse_args()

    if options.sampling_mode == "prob_num":
        options.sampling_param = ast.literal_eval(options.sampling_param)
        if not isinstance(options.sampling_param, tuple):
            print >>sys.stderr, (
                    "ERROR: sampling_param should evaluate to a tuple.")
            parser.print_help()
            exit(-1)

    if options.rand_comp_input_file:
        with open(options.rand_comp_input_file, "r") as rc_file:
            rc_dict = pickle.load(rc_file)

            if "components" in rc_dict:
                # Parameter was a knowledge_params.pickle file, as written by
                # accuracy_model_train.py
                rc_dict = rc_dict["components"]

            rand_features.set_components(rc_dict)

            # It is very important to turn dynamic mode back on, because we
            # want to union the components together, not only use the old
            rand_features.dynamic_mode = True
            print >>sys.stderr, "Random components read from input file."

    return options


def main():

    # Seed the random number generator so experiments are repeatable
    random.seed(909090)
    np.random.seed(909090)

    options = get_cmd_line_options()

    prev_user = None
    attempts = []

    for line in sys.stdin:
        row = acc_util.linesplit.split(line.strip())

        user = row[idx.user]
        if user != prev_user:
            # We're getting a new user, so perform the reduce operation
            # on all the attempts from the previous user
            emit_samples(attempts, options)
            attempts = []

        # Convert from Hive to Python formats
        row[idx.correct] = row[idx.correct] == 'true'
        row[idx.problem_number] = int(row[idx.problem_number])

        # Note, this is in scientific notation, so be careful
        row[idx.time_done] = float(row[idx.time_done])

        # TODO(mattfaus): This is broken
        if options.sampling_mode == 'random':
            row[idx.scheduler_info] = json.loads(
                    row[idx.scheduler_info])

        attempts.append(row)

        prev_user = user

    emit_samples(attempts, options)

    if options.rand_comp_output_file:
        rand_features.write_components(options.rand_comp_output_file)

    print >>sys.stderr, "%d valid samples." % emitted_samples
    history_error_rate = (float(error_invalid_history) / (error_invalid_history
        + emitted_samples)) * 100
    print >>sys.stderr, "%d history errors (%.2f%%)." % (error_invalid_history,
         history_error_rate)

if __name__ == '__main__':
    main()
