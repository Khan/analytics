"""Data preprocessing script for UserAssessment data.

This script can read data file of UserAssessment data downloaded from S3,
preprocess it, and output it in a format suitable for use with mirt_train.py
or random_features.py.

Each line in the input file is the URL-safe datastore key, followed by either
a tab character or binary value of 1, followed by a JSON-encoded (via
api.jsonify) UserAssessment object as defined in
website/assessment/assessment_models.py. E.g.,

'73feb9c8c\t{"type":"UserAssessment","exercise":"addition_1",...'

Note that the value of the UserAssessment's "history" field is itself
JSON-encoded.

Example usage:

  # First get the raw data to pass to this script
  # TODO(jace) : make this an automatic part of this script.
  mkdir mirt_data
  s3cmd get --recursive \
    s3://ka-mapreduce/entity_store/UserAssessmentP/dt=2013-03-11/ mirt_data
  cd mirt_data

  # Now pipe the data to this script
  gzcat *.gz | python [this file] > user_assessment.responses

"""

import collections
import fileinput
import json
import re
import sys


def preprocess():

    slug_counts = collections.defaultdict(int)
    (complete_count, valid_count, total_count) = (0, 0, 0)

    linesplitter = re.compile('[\t\x01]')

    for line_str in fileinput.input():

        key, json_str = linesplitter.split(line_str.strip(), maxsplit=2)

        user_assessment = json.loads(json_str)
        history = json.loads(user_assessment['history'])

        # tally some assesment-level stats  -- these are just for
        # debugging and sanity checking data quality
        slug_counts[user_assessment['slug']] += 1
        total_count += 1
        # HACK(jace): 15 is the question length of all assessments
        # we've used so far.  Again, this is just for debugging.
        if len(history) == 15:
            complete_count += 1
        if sum(r['correct'] for r in history) > 3:
            valid_count += 1

        problem_nums = collections.defaultdict(int)

        for response in history:
            problem_nums[response['exercise']] += 1

            # formatting to match output of hive queries
            hive_bool = lambda b: "true" if b else "false"

            # the following conforms to the input format expected by
            # mirt_train_EM.py
            outline = (key + ","  # use assessment key instead of user
                    + response['time_done'] + ","
                    + 'problemlog' + ","  # not really a plog, but similar
                    + response['exercise'] + ","
                    + response['problem_type'] + ","
                    + response['seed'] + ","
                    + "%d" % response['time_taken'] + ","
                    + "%d" % problem_nums[response['exercise']] + ","
                    + hive_bool(response['correct']) + ","
                    + "1" + ","  # count_attempts
                    + "0" + ","  # count_hints
                    + hive_bool(response['correct']) + ","  # eventually_crrct
                    + "false" + ","  # topic_mode
                    + "None" + ","  # key
                    + "None" + ","  # dt
                    )

            print outline

    # debugging/informational output
    print >> sys.stderr, (total_count, complete_count, valid_count)
    print >> sys.stderr, json.dumps(slug_counts, indent=4)


if __name__ == '__main__':
    preprocess()
