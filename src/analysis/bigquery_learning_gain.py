"""
Using BigQuery to do a quick-n-dirty learning gain approximation.

This also serves as an experiment in using the BigQuery Python API.

Setup notes:
- install the python client:
    pip install google-api-python-client
- put the project ID in a file, say "id.txt"
- download the "clients_secrets.json" file for the khan project
- run!
    python bigquery_learning_gain.py
"""

import httplib2
import os
import pprint
import time

from apiclient.discovery import build
from apiclient.errors import HttpError

from oauth2client.client import AccessTokenRefreshError
from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage
from oauth2client.tools import run


PROJECT_ID_FILE = 'id.txt'
CREDENTIALS_FILE = 'bigquery_credentials.dat'
CLIENT_SECRETS_FILE = os.path.expanduser('~/khan/clients_secrets.json')


def get_project_id():
    # The project ID lives in id.txt (to avoid having this in plaintext)
    f = open(PROJECT_ID_FILE, 'r')
    project_id = f.readline().strip()
    return project_id


def get_bigquery_service():
    storage = Storage(CREDENTIALS_FILE)
    credentials = storage.get()

    if credentials is None or credentials.invalid:
        flow = flow_from_clientsecrets(
            CLIENT_SECRETS_FILE,
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
                time.sleep(1.0)

    except HttpError as err:
        print 'Error:', pprint.pprint(err.content)

    except AccessTokenRefreshError:
        print ("Credentials have been revoked or expired, please re-run"
         "the application to re-authorize")


def generate_learning_gain(service, project_id, experiment_name,
                           start_date, backup_date):
    # Query 1:
    # TODO(tony): generate analytics_cards_1 table (rename?)
    # Note: only need to do this once (and it's the most expensive)

    # Query 2: exp_analytics
    query_string = (
        'SELECT user_id, correct, time_done\n'
        'FROM [tony.analytics_cards_1]\n'
        'WHERE task_type = \'mastery.analytics\'\n'
        'AND time_done >= TIMESTAMP(\'%s 00:00:00\')\n'
    ) % start_date
    run_query(service, project_id, query_string, 'exp_analytics')

    # Query 3: exp_analytics_min_max
    query_string = (
        'SELECT user_id,\n'
        '  MIN(time_done) AS min_time,\n'
        '  MAX(time_done) AS max_time\n'
        'FROM [tony.exp_analytics]\n'
        'GROUP BY user_id\n'
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
        '  AVG(last_correct - first_correct) AS delta,\n'
        '  COUNT(alternative) AS n\n'
        'FROM [tony.exp_analytics_first_last]\n'
        'WHERE alternative != "null"\n'
        'GROUP BY alternative ORDER BY alternative\n'
    )
    run_query(service, project_id, query_string, 'exp_results')


def main():
    service = get_bigquery_service()
    project_id = get_project_id()
    generate_learning_gain(service, project_id,
        'Review scheduling methods',
        '2013-11-01', '2014_02_08')


if __name__ == '__main__':
    main()
