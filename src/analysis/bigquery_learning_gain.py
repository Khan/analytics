"""
Using BigQuery to do a quick-n-dirty learning gain approximation.

This also serves as an experiment in using the BigQuery Python API.
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


def main():
    bigquery_service = get_bigquery_service()

    try:
        datasets = bigquery_service.datasets()
        project_id = get_project_id()
        list_reply = datasets.list(projectId=project_id).execute()
        print 'Dataset list:'
        pprint.pprint(list_reply)
    except HttpError as err:
        print 'Error in listDatasets:', pprint.pprint(err.content)
    except AccessTokenRefreshError:
        print ("Credentials have been revoked or expired, please re-run"
         "the application to re-authorize")


if __name__ == '__main__':
    main()
