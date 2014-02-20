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

    bigquery_service = build('bigquery', 'v2', http=http)

    return bigquery_service


def run_query(query_string, destination_table, allow_large_results=True):
    pass


def main():
    bigquery_service = get_bigquery_service()
    project_id = get_project_id()

    try:
        query_request = bigquery_service.jobs()
        query_data = {'query': (
                'SELECT TOP( title, 10) as title, COUNT(*) as revision_count'
                ' FROM [publicdata:samples.wikipedia] WHERE wp_namespace = 0;'
            )
        }
        query_response = query_request.query(projectId=project_id,
                                             body=query_data).execute()
        print 'Query Results:'
        for row in query_response['rows']:
            result_row = []
            for field in row['f']:
                result_row.append(field['v'])
            print ('\t').join(result_row)

    except HttpError as err:
        print 'Error:', pprint.pprint(err.content)

    except AccessTokenRefreshError:
        print ("Credentials have been revoked or expired, please re-run"
         "the application to re-authorize")


if __name__ == '__main__':
    main()
