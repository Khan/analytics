"""Analyze the frequency of specific responses to execise questions.

Computes the top 10 most common answers to each question in the ProblemLogs,
and prints that data out in CSV format.  Handles both Perseus and
khan-exercises style exercises.

Note: This needs to be run on the 'analytics' EC2 machine, becuase the
entity_loader module reads the data from local storage.  To run, simply ssh
into the analytics machine, and execute:

cd ~/analytics/src
python analytics/exercsise_answer_counts.py

"""

from collections import defaultdict
import csv
import datetime
import sys

import entity_loader

# TODO(user)  Config values here.  Tweak them as you wish.
perseus_only = True
begin_date = datetime.date(2013, 7, 10)
end_date = datetime.date(2013, 8, 10)

# responses = {exercise: {seed: {response: observation_count}}}
responses = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
sha1s = {}  # stores the most recent sha1 for content
err_count = khan_ex_count = perseus_count = sha1_tosses = unicode_err = 0

loader = entity_loader.EntityLoader()
for log in loader.entities("ProblemLog", end_date, begin_date, limit=None):
    data = log.get('json')

    # Data validation
    if ('attempts' not in data or
            not isinstance(data['attempts'], list) or
            not data['attempts']):  # empty list
        err_count += 1
        continue

    seed = data['seed']
    first_attempt = data['attempts'][0]

    if data['seed'].startswith('x'):
        perseus_count += 1
        exercise = data['exercise']
        sha1_key = seed
    else:
        khan_ex_count += 1
        exercise = data['exercise'] + ":" + str(data['problem_type'])
        sha1_key = data['exercise']
        if perseus_only:
            continue

    if khan_ex_count + perseus_count % 1e6 == 0:
        print "Processed %d logs." % (khan_ex_count + perseus_count)

    # We proceed in reverse chronological order.  If we detect a
    # changed sha1, we ignore subsequent (older) data, so the stats
    # will only include data from the latest version of the item/exercise.
    if sha1s.setdefault(sha1_key, data['sha1']) != data['sha1']:
        sha1_tosses += 1
        continue

    # Note: only counting first attempt, but this is easily modified.
    responses[exercise][seed][first_attempt] += 1

# Print some pretty output in CSV format.
writer = csv.writer(sys.stdout)
sorted_exs = sorted(responses.keys())
for ex in sorted_exs:
    seeds = responses[ex]
    for seed, stats in seeds.iteritems():
        stats = [t for t in stats.iteritems()]
        stats = sorted(stats, reverse=True, key=lambda t: t[1])
        for response, count in stats[:10]:
            try:
                writer.writerow([ex, seed, response, count])
            except UnicodeEncodeError:
                unicode_err += 1  # TODO(jace) handle unicode stuff better.

print "Error count: ", err_count
print "Unicode errors: ", unicode_err
print "SHA1 throwaways: ", sha1_tosses
print "khan-execises count: ", khan_ex_count
print "Perseus count: ", perseus_count
