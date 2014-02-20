"""
Using BigQuery to do a quick-n-dirty learning gain approximation.

This also serves as an experiment in using the BigQuery Python API.
"""

import httplib2
import pprint

from apiclient.discovery import build
from apiclient.errors import HttpError

from oauth2client.client import AccessTokenRefreshError
from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage
from oauth2client.tools import run


def get_project_id():
    # The project ID lives in id.txt (to avoid having this in plaintext)
    f = open('id.txt', 'r')
    return f.readline().strip()

# Enter your Google Developer Project number
PROJECT_NUMBER = get_project_id()
FLOW = flow_from_clientsecrets('/Users/tony/khan/client_secrets.json',
                               scope='https://www.googleapis.com/auth/bigquery')


def main():
    storage = Storage('bigquery_credentials.dat')
    credentials = storage.get()

    if credentials is None or credentials.invalid:
        credentials = run(FLOW, storage)

    http = httplib2.Http()
    http = credentials.authorize(http)

    bigquery_service = build('bigquery', 'v2', http=http)

    try:
        datasets = bigquery_service.datasets()
        listReply = datasets.list(projectId=PROJECT_NUMBER).execute()
        print 'Dataset list:'
        pprint.pprint(listReply)

    except HttpError as err:
        print 'Error in listDatasets:', pprint.pprint(err.content)

    except AccessTokenRefreshError:
        print ("Credentials have been revoked or expired, please re-run"
         "the application to re-authorize")


if __name__ == '__main__':
    main()

