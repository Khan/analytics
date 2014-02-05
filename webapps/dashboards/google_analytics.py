"""This module provides access to Google Analytics data.

The OAuth2.0 Client is expected to be a service account that is
authorized using a secret private key. In this way, viewers don't need
to individually auth against Google Analytics' API.


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
import json
import os.path

import apiclient.discovery
import apiclient.model
import oauth2client.client
import oauth2client.file
import oauth2client.tools

# The file with the OAuth 2.0 Client details for authentication and
# authorization. When created, this module used "Service Account" that
# chris@ created on the khan-academy cloud project.
CLIENT_SECRETS = os.path.join(os.path.dirname(__file__),
                              "ga_client_secrets.json")

# The file with the private key that matches the Service Account
# client details in CLIENT_SECRETS.
PRIVATE_KEY = os.path.join(os.path.dirname(__file__),
                           "ga_client_privatekey.p12")

# The OAuth 2.0 Scope used to access Google Analytics' API.
SCOPE = "https://www.googleapis.com/auth/analytics.readonly"


def _prepare_credentials():
    """Build credentials for a service account client from its private key."""
    with open(CLIENT_SECRETS, "r") as client_secrets_file:
        client_secrets = json.load(client_secrets_file)

    with open(PRIVATE_KEY, "rb") as private_key_file:
        credentials = oauth2client.client.SignedJwtAssertionCredentials(
            service_account_name=client_secrets["web"]["client_email"],
            private_key=private_key_file.read(),
            scope=SCOPE)
        return credentials


def initialize_service(raw_model=False):
    """Return authorized service object for Google Analytics."""
    http = httplib2.Http()
    credentials = _prepare_credentials()
    http = credentials.authorize(http)
    # TODO(chris): if accessed frequently, this should be cached rather
    # instead of making HTTP calls on every request.
    return apiclient.discovery.build("analytics", "v3", http=http)
