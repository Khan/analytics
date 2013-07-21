# From https://github.com/khan/khan-api

import cgi
import logging
import urllib2
import urlparse
import webbrowser

from oauth import OAuthConsumer, OAuthToken, OAuthRequest, OAuthSignatureMethod_HMAC_SHA1

class TestOAuthClient(object):

    def __init__(self, server_url, consumer_key, consumer_secret):
        self.server_url = server_url
        self.consumer = OAuthConsumer(consumer_key, consumer_secret)

    def start_fetch_request_token(self):
        oauth_request = OAuthRequest.from_consumer_and_token(
                self.consumer,
                http_url = "%s/api/auth/request_token" % self.server_url
                )

        oauth_request.sign_request(OAuthSignatureMethod_HMAC_SHA1(), self.consumer, None)
        webbrowser.open(oauth_request.to_url())

    def fetch_access_token(self, request_token):

        oauth_request = OAuthRequest.from_consumer_and_token(
                self.consumer,
                token = request_token,
                http_url = "%s/api/auth/access_token" % self.server_url
                )

        oauth_request.sign_request(OAuthSignatureMethod_HMAC_SHA1(), self.consumer, request_token)

        response = get_response(oauth_request.to_url())

        return OAuthToken.from_string(response)

    def access_resource(self,
                        relative_url,
                        access_token,
                        method="GET",
                        params=None):

        full_url = self.server_url + relative_url
        url = urlparse.urlparse(full_url)
        full_params = cgi.parse_qs(url.query)
        for key in full_params:
            full_params[key] = full_params[key][0]

        if params:
            full_params.update(params)

        oauth_request = OAuthRequest.from_consumer_and_token(
                self.consumer,
                token = access_token,
                http_url = full_url,
                parameters = full_params,
                http_method=method
                )

        oauth_request.sign_request(OAuthSignatureMethod_HMAC_SHA1(), self.consumer, access_token)

        if method == "GET":
            response = get_response(oauth_request.to_url())
        else:
            response = post_response(full_url, oauth_request.to_postdata())

        return response

    def post_resources(self,
                       relative_url,
                       access_token,
                       method="POST",
                       data=None,
                       content_type=None):
        full_url = self.server_url + relative_url

        oauth_request = OAuthRequest.from_consumer_and_token(
                self.consumer,
                token = access_token,
                http_url = full_url,
                http_method=method
                )

        oauth_request.sign_request(OAuthSignatureMethod_HMAC_SHA1(), self.consumer, access_token)

        return post_response(oauth_request.to_url(), data, content_type)



def get_response(url):
    response = ""
    file = None
    try:
        file = urllib2.urlopen(url)
        response = file.read()
    finally:
        if file:
            file.close()

    return response

def post_response(url, data, content_type=None):
    response = ""
    file = None
    headers = {}

    if content_type:
        headers = {'Content-Type': content_type}

    req = urllib2.Request(url, data, headers)

    try:
        file = urllib2.urlopen(req)
        response = file.read()
    finally:
        if file:
            file.close()

    return response
