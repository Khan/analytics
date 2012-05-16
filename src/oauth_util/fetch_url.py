"""A convenience routine used to create and use an oauth connection.

This requires access_token.py be populated (see get_access_token.py).
It does not try to do the oauth authentication itself, but uses this
hard-coded authentication token with the oauth library.
"""

import sys

import consts
import oauth
import test_oauth_client
try:
    from access_token import ACCESS_TOKEN_RESPONSE
except ImportError:
    sys.exit("""\
Run 'cd analytics/src/oauth_util && ./get_access_token.py' to
generate an OAuth token.  You will probably need to run it from your
local machine (since it requires a web browser), and then manually
update the contents of oauth_util/access_token.py on this machine.
""")


def fetch_url(url_path):
    """url_path is like '/api/v1/users'.  Hostname is taken from consts.py."""
    client = test_oauth_client.TestOAuthClient(
        consts.SERVER_URL, consts.CONSUMER_KEY, consts.CONSUMER_SECRET)

    access_token = oauth.OAuthToken.from_string(ACCESS_TOKEN_RESPONSE)

    return client.access_resource(url_path, access_token)
