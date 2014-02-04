"""This module provides access to Google Analytics data.

This isn't intended for per-user or multi-user access. One user will
auth against Google Analytics, and the temporary access token will be
stored locally.

USAGE:

  import google_analytics
  analytics_service = google_analytics.initialize_service()

  # See the "Dimensions & Metrics Reference" for info about parameters:
  #   https://developers.google.com/analytics/devguides/reporting/core/dimsmets

  response = analytics_service.data().ga().get(
    ids="ga:XXX",  # Google Analytics View ID for your project
    start_date="2014-01-01",
    end_date="2014-01-31",
    dimensions="ga:date",
    metrics="ga:avgDOMContentLoadedTime",
    filters="ga:pagePath==/").execute()
  print response

"""

import httplib2

import apiclient.discovery
import apiclient.model
import oauth2client.client
import oauth2client.file
import oauth2client.tools

# The file with the OAuth 2.0 Client details for authentication and
# authorization. When created, this module used "Client ID for native
# application" that chris@ created on the khan-academy cloud project.
CLIENT_SECRETS = "ga_client_secrets.json"

# A helpful message to display if the CLIENT_SECRETS file is missing.
MISSING_CLIENT_SECRETS_MESSAGE = ('%s is missing. Download a "Client ID '
                                  'for native application" from the KA '
                                  'cloud project' % CLIENT_SECRETS)

# The Flow object to be used if we need to authenticate.
FLOW = oauth2client.client.flow_from_clientsecrets(
    CLIENT_SECRETS,
    scope="https://www.googleapis.com/auth/analytics.readonly",
    message=MISSING_CLIENT_SECRETS_MESSAGE)

# A file to store the access token. This isn't a great storage method,
# and doesn't work for multiple users, but it's sufficient for now to
# auth against one user and store their info.
TOKEN_FILE_NAME = "google_analytics_access.token"


def _prepare_credentials():
    """Read credentials from the file system or auth the user."""
    storage = oauth2client.file.Storage(TOKEN_FILE_NAME)
    credentials = storage.get()

    # If existing credentials are invalid, run the auth flow. The run
    # method will store any new credentials.
    if not credentials or credentials.invalid:
        credentials = oauth2client.tools.run(FLOW, storage)

    return credentials


def initialize_service(raw_model=False):
    """Return authorized service object for Google Analytics."""
    http = httplib2.Http()
    credentials = _prepare_credentials()
    http = credentials.authorize(http)
    # TODO(chris): if accessed frequently, this should be cached rather
    # instead of making HTTP calls on every request.
    return apiclient.discovery.build("analytics", "v3", http=http)
