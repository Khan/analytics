#!/usr/bin/env python

"""A quick'n'dirty script to get an OAuth access token, modified from
https://github.com/khan/khan-api
"""

from test_oauth_client import TestOAuthClient
from oauth import OAuthToken
import datetime as dt

import consts

def get_access_token_from_user():
    client = TestOAuthClient(consts.SERVER_URL, consts.CONSUMER_KEY, consts.CONSUMER_SECRET)
    client.start_fetch_request_token()

    print "After logging in and authorizing, input token key and secret..."

    request_token_key = raw_input("request token: ")
    request_token_secret = raw_input("request token secret: ")

    request_token = OAuthToken(request_token_key, request_token_secret)
    if not request_token:
        raise IOError("Did not get request token.")

    print "Fetching access token..."
    access_token = client.fetch_access_token(request_token)
    if not access_token:
        raise IOError("Did not get access token.")

    return access_token

def main():
    access_token = get_access_token_from_user()

    f = open('access_token.py', 'w')
    f.write('# This file automatically generated on %s\n'
            'ACCESS_TOKEN_RESPONSE = "%s"\n' % (dt.datetime.now(), access_token))
    f.close()

    print "Done. Response written to access_token.py."

if __name__ == '__main__':
    main()
