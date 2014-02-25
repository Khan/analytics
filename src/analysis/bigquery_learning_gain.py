"""
Using BigQuery to do a quick-n-dirty learning gain approximation.

This also serves as an experiment in using the BigQuery Python API.

Setup notes:
- install the python client and bigquery:
    pip install bigquery
    pip install google-api-python-client
- put the project ID in a file, say "id.txt"
- download the "clients_secrets.json" file for the khan project
- run!
    python bigquery_learning_gain.py
"""

import datetime
import httplib2
import os
import pprint
import re
import time

from apiclient.discovery import build
from apiclient.errors import HttpError

from oauth2client.client import AccessTokenRefreshError
from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage
from oauth2client.tools import run

from numpy import sqrt
from scipy import stats
from pandas.io import gbq


PROJECT_ID_FILE = 'id.txt'
CREDENTIALS_FILE = 'bigquery_credentials.dat'
CLIENT_SECRETS_FILE = os.path.expanduser('~/khan/clients_secrets.json')


def get_project_id():
    # The project ID lives in id.txt (to avoid having this in plaintext)
    f = open(PROJECT_ID_FILE, 'r')
    project_id = f.readline().strip()
    return project_id


def get_bigquery_service(client_secrets_file=CLIENT_SECRETS_FILE):
    storage = Storage(CREDENTIALS_FILE)
    credentials = storage.get()

    if credentials is None or credentials.invalid:
        flow = flow_from_clientsecrets(
            client_secrets_file,
            scope='https://www.googleapis.com/auth/bigquery'
        )
        credentials = run(flow, storage)

    http = httplib2.Http()
    http = credentials.authorize(http)

    service = build('bigquery', 'v2', http=http)

    return service


def run_query(service, project_id, query_string,
              destination_table, allow_large_results=True):
    print '\nQuery:\n%s' % query_string
    # Must use the async version to specify configuration
    query_data = {
        'configuration': {
            'query': {
                'destinationTable': {
                    'projectId': project_id,
                    'tableId': destination_table,
                    'datasetId': 'tony',  # TODO(tony): don't hard-code this
                },
                'priority': 'INTERACTIVE',
                'writeDisposition': 'WRITE_TRUNCATE',  # Overwrite
                'allowLargeResults': allow_large_results,
                'createDisposition': 'CREATE_IF_NEEDED',
                'query': query_string,
            },
        },
    }

    try:
        query_request = service.jobs()
        query_response = query_request.insert(projectId=project_id,
                                              body=query_data).execute()

        # Poll until this is done...
        start_time = time.time()
        while True:
            query_response = query_request.get(
                projectId=project_id,
                jobId=query_response['jobReference']['jobId']
            ).execute()
            status = query_response['status']['state']

            print 'Elapsed: %.1fs, status: %s' % (
                time.time() - start_time, status)

            if status == 'DONE':
                break
            else:
                time.sleep(2.0)
        return query_response

    except HttpError as err:
        print 'Error:', pprint.pprint(err.content)

    except AccessTokenRefreshError:
        print ("Credentials have been revoked or expired, please re-run"
         "the application to re-authorize")


def get_start_date_for_experiment(service, project_id, experiment_name,
                                  backup_date):
    # Note that we can't reliably get the date from GAE Bingo since there's
    # a bug when new conversions are added and they are first alphabetically.
    # Instead, let's just run a query...

    # We assume that the earliest conversion (that hasn't changed) is
    # actually we the A/B test started. Reasonable.
    print 'Getting start date for experiment:', experiment_name
    query_request = service.jobs()
    query_string = (
        'SELECT MIN(dt_started) as start_date\n'
        'FROM [%s._GAEBingoExperiment]\n'
        'WHERE canonical_name = \'%s\'\n'
        'GROUP BY canonical_name\n'
    ) % (backup_date, experiment_name)

    query_response = query_request.query(projectId=project_id,
        body={
            'query': query_string
        }
    ).execute()

    for row in query_response['rows']:
        for field in row['f']:
            usec = int(field['v'])
            ts = usec / 1000 / 1000
            start_date = datetime.datetime.fromtimestamp(ts)
            return start_date
    # Should never get here!
    raise


def get_most_recent_backup_date(service, project_id):
    # Get most recent dataset that matches YYYY_MM_DD
    datasets = service.datasets()
    datasets_list = datasets.list(projectId=project_id).execute()
    backup_date = None
    for d in datasets_list['datasets']:
        dataset_name = d['datasetReference']['datasetId']
        # Must match YYYY_MM_DD format
        if re.search('(^[0-9]{4}_[0-9]{2}_[0-9]{2}$)', dataset_name):
            # Take the most recent date
            if backup_date is None or dataset_name > backup_date:
                backup_date = dataset_name
    assert backup_date
    print 'Backup date:', backup_date
    return backup_date


def generate_analytics_cards(service, project_id, backup_date):
    # We generate the analytics cards from the ProblemLog table,
    # only if necessary. Note that this query is expensive, so
    # we only do this once (and subsequent queries can just look
    # at the analytics cards - much smaller).
    #
    # Let's look at the most recently done analytics card
    # and if it's close enough to the backup date, assume
    # that we don't need to regenerate the table.
    results = gbq.read_gbq('SELECT MAX(time_done) as max_time \n'
                           'FROM [tony.analytics_cards_1]',
                           project_id=project_id)
    usec = int(results['max_time'][0])
    ts = usec / 1000 / 1000
    last_ac_time = datetime.datetime.fromtimestamp(ts)
    last_date = datetime.datetime.strptime(backup_date, '%Y_%m_%d')
    gap = last_date - last_ac_time
    gap_in_days = gap.days
    print 'Analytics card table is', gap, 'old'
    if gap_in_days > 1:
        print 'Regenerating analytics_cards_1...'
        query_string = (
            'SELECT user_id, task_type, time_done, exercise, correct\n'
            'FROM [%s.ProblemLog]\n'
            'WHERE task_type = \'mastery.analytics\'\n'
        ) % backup_date
        run_query(service, project_id, query_string, 'analytics_cards_1')
    else:
        print 'Table is up to date. Skipping...'


def read_results(project_id):
    # Assumes the table is available!
    results = gbq.read_gbq('SELECT * FROM [tony.exp_results]',
                           project_id=project_id)
    return results


class LearningGainResults:
    def __init__(self, experiment_name, data, start_date, days_old):
        self.experiment_name = experiment_name
        self.data = data
        self.start_date = start_date
        self.days_old = days_old

    # TODO(tony): implement helpers
    def is_significant(self):
        pass

    pass


def generate_learning_gain(service, project_id, backup_date,
                           experiment_name):
    # Get start date from experiment name
    start_date = get_start_date_for_experiment(service, project_id,
        experiment_name, backup_date)
    last_date = datetime.datetime.strptime(backup_date, '%Y_%m_%d')
    days_old = (last_date - start_date).days

    print '\nRunning learning gain...'
    print 'Bucket: %s\n' % backup_date

    print experiment_name
    print 'Started: %s (%d days old)' % (start_date.date(), days_old)

    # Query 1: generate analytics_cards_1
    # This is now done in generate_analytics_cards

    # Query 2: exp_analytics
    query_string = (
        'SELECT user_id, correct, time_done\n'
        'FROM [tony.analytics_cards_1]\n'
        'WHERE task_type = \'mastery.analytics\'\n'
        'AND time_done >= TIMESTAMP(\'%s\')\n'
    ) % start_date
    run_query(service, project_id, query_string, 'exp_analytics')

    # Query 3: exp_analytics_min_max
    query_string = (
        'SELECT user_id,\n'
        '  MIN(time_done) AS min_time,\n'
        '  MAX(time_done) AS max_time\n'
        'FROM [tony.exp_analytics]\n'
        'GROUP BY user_id\n'
        'HAVING min_time != max_time\n'
    )
    run_query(service, project_id, query_string, 'exp_analytics_min_max')

    # Query 4: exp_user_bingo
    query_string = (
        'SELECT ud.user_id AS user_id, bm.alternative AS alternative\n'
        'FROM\n'
        '  (SELECT user_id, gae_bingo_identity\n'
        '   FROM [%s.UserData]) AS ud\n'  # TODO(tony): replace date
        '  JOIN EACH\n'
        '    (SELECT bingo_id, alternative\n'
        '     FROM [jace.bingo_map]\n'
        '     WHERE canonical_name=\'%s\') AS bm\n'
        '    ON bm.bingo_id=ud.gae_bingo_identity\n'
    ) % (backup_date, experiment_name)
    run_query(service, project_id, query_string, 'exp_user_bingo')

    # Query 5: exp_analytics_first_last
    query_string = (
        'SELECT amm.user_id AS user_id, ub.alternative AS alternative,\n'
        '  ap1.correct AS first_correct, ap2.correct AS last_correct\n'
        'FROM [tony.exp_analytics_min_max] AS amm\n'
        'LEFT JOIN EACH [tony.exp_analytics] AS ap1\n'
        '  ON ap1.time_done=amm.min_time AND ap1.user_id=amm.user_id\n'
        'LEFT JOIN EACH [tony.exp_analytics] AS ap2\n'
        '  ON ap2.time_done=amm.max_time AND ap2.user_id=amm.user_id\n'
        'LEFT JOIN EACH [tony.exp_user_bingo] AS ub\n'
        '  ON ub.user_id=amm.user_id\n'
    )
    run_query(service, project_id, query_string, 'exp_analytics_first_last')

    # Query 6: exp_results
    query_string = (
        'SELECT alternative,\n'
        '  ROUND(100 * AVG(last_correct - first_correct), 5) AS delta,\n'
        '  ROUND(100 * STDDEV(last_correct - first_correct) / SQRT(n), 5)\n'
        '    as stderr,\n'
        '  ROUND(100 * AVG(first_correct), 3) as first,\n'
        '  ROUND(100 * AVG(last_correct), 3) as last,\n'
        '  COUNT(alternative) AS n\n'
        'FROM [tony.exp_analytics_first_last]\n'
        'WHERE alternative != "null"\n'
        'GROUP BY alternative ORDER BY alternative\n'
    )
    run_query(service, project_id, query_string, 'exp_results')

    # Get results
    print '\nResults ready!\n'
    print experiment_name
    print 'Started: %s (%d days old)' % (start_date.date(), days_old)

    data = read_results(project_id)
    print data

    # Wrap it up
    results = LearningGainResults(experiment_name, data, start_date, days_old)
    return results


def calculate_prob(n1, n2, m1, m2, s1, s2):
    # Calculates the probability that the first sample's mean is greater than
    # the second. We assume unequal variances but unequal sample sizes. We use
    # Welch's t-test here (the sample sizes are huge; same as z-test).
    z = 1.0 * (m1 - m2) / sqrt(s1 * s1 + s2 * s2)
    # print 'z:', z
    p = stats.norm.cdf(z)
    # print 'p:', p
    return p


def generate_report(backup_date, all_results):
    print 'Begin report...\n'
    for results in all_results:
        text = ''

        data = results.data
        i = data['delta'].argmax()
        best_alternative = data['alternative'][i]

        text += results.experiment_name + '\n'
        text += 'Started: %s (%d days old)\n' % (results.start_date.date(),
                                               results.days_old)
        text += '\n' + str(data) + '\n\n'
        text += 'The best alternative is [%s]!\n' % best_alternative
        for j in xrange(len(data)):
            if j == i:
                continue
            n1, n2 = data['n'][i], data['n'][j]
            m1, m2 = data['delta'][i], data['delta'][j]
            s1, s2 = data['stderr'][i], data['stderr'][j]
            prob = calculate_prob(n1, n2, m1, m2, s1, s2)
            text += ('The probability this is better than [%s]'
                     ' is %.2f%%.\n' % (
                     data['alternative'][j], prob * 100.0))

        print text


def main():
    # TODO(tony): add command-line args
    # -v, --verbose
    # -e, --experiments (file)

    service = get_bigquery_service()
    project_id = get_project_id()
    backup_date = get_most_recent_backup_date(service, project_id)

    # Update the analytics cards table if necessary
    generate_analytics_cards(service, project_id, backup_date)

    # Generate the results for all experiments
    experiments = [
        # 'Review scheduling methods',
        # 'adaptive pretest question difficulty cutoff',
        # 'Athena: mastery task length v2',
        # 'Mastery Tasks: Challenge Card Enforce Prerequisites',
        # 'Mastery Tasks: Challenge Card Selection Aggressiveness',
        # 'Mastery Tasks: progress card ordering',
        # 'metacognitive 2',
        # 'metacognitive 2 prompt type',
        # 'metacognitive 2 text',

        # 'Practice Tasks: Curation Strategy',
        # 'Practice Tasks: Knowledge Map Strategies',
        # 'Practice Tasks: Suggestion Strategy',

        # 'pretest utility function - difficulty',
        # 'pretest utility function - time',
        'Pretest: parameterization',
        # 'Pretest: promoter aggressiveness',
    ]
    all_results = []
    for e in experiments:
        results = generate_learning_gain(service, project_id, backup_date, e)
        all_results.append(results)

    generate_report(backup_date, all_results)


if __name__ == '__main__':
    main()
