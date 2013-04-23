"""Script to populate the list of exercises defining each assessment.

We build an observed mapping of assessment slugs to exercise IDs from
looking at a historic record of assessment activity in the form of
JSONified UserAssessment objects.

USAGE:

  # First get the raw data to pass to this script
  mkdir mirt_data && cd mirt_data
  s3cmd get --recursive \
    s3://ka-mapreduce/entity_store/UserAssessmentP/dt=2013-03-11/

  # Now pipe the data to this script
  gzcat *.gz | python assessment_defs_extractor.py >assessment_defs.json


Sample output:
    {
        "__comment": "Comments aren't part of the data",
        "algebra": [
            "absolute_value",
            "adding_and_subtracting_complex_numbers",
            "adding_and_subtracting_decimals_word_prob",
            ...
        ],
        "fractions": [
            "adding_and_subtracting_fractions",
            "adding_fractions",
            "adding_fractions_with_common_denominators",
            ...
        ],
        ...
    }


As input this script takes a data file of the Hive UserAssessmentP table
from S3.  Each line in the input file is the URL-safe datastore key,
followed by either a tab character or binary value of 1, followed by a
JSON-encoded (via api.jsonify) UserAssessment object as defined in
website/assessment/assessment_models.py. E.g.,

ag5zhIVFcnjQM\t{"slug":"algebra","history":"[{\"exercise\":\"addition_1\",...

Note that the value of the UserAssessment's "history" field is itself
JSON-encoded.

"""

import fileinput
import json
import re


def main():
    linesplitter = re.compile('[\t\x01]')
    slug_to_exercises = {}
    for line_str in fileinput.input():
        key, json_str = linesplitter.split(line_str.strip(), maxsplit=2)
        user_assessment = json.loads(json_str)
        slug = user_assessment['slug']
        history = json.loads(user_assessment['history'])
        for h in history:
            slug_to_exercises.setdefault(slug, set()).add(h['exercise'])
    # Order alphabetically for easier hand-editing of exercises.
    for slug in slug_to_exercises.keys():
        slug_to_exercises[slug] = sorted(slug_to_exercises[slug])
    slug_to_exercises['__comment'] = (
        "This configuration maps each assessment slug to the set of "
        "exercises that the assessment is composed of.")
    print json.dumps(slug_to_exercises, sort_keys=True, indent=4)

if __name__ == '__main__':
    main()
