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
    # Must use the async version to specify configuration
    query_request = service.jobs()
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
            if status == 'DONE':
                print 'Total elapsed: %3f' % (time.time() - start_time)
                break
            else:
                print 'Sleeping... elapsed: %.3f, current status: %s' % (
                    time.time() - start_time, status)
                time.sleep(1.0)

    except HttpError as err:
        print 'Error:', pprint.pprint(err.content)

    except AccessTokenRefreshError:
        print ("Credentials have been revoked or expired, please re-run"
         "the application to re-authorize")


def generate_learning_gain(service, project_id, experiment_name,
                           start_date, backup_date):
    # Query 1:
    pass


def main():
    service = get_bigquery_service()
    project_id = get_project_id()

    query_string = (
        'SELECT *'
        ' FROM [tony.exp_results]'
    )
    run_query(service, project_id, query_string, 'tmp')


if __name__ == '__main__':
    main()
