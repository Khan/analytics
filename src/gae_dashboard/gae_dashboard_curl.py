#!/usr/bin/env python

"""Download the contents of an AppEngine admin UI page.

Usage: echo GAE_PASSWORD | gae_dashboard_curl.py URL GAE_USERNAME

The URL must start with "https://appengine.google.com/" or can be a
server-relative URL, e.g., /instances.

The URL's contents are printed to standard output or an error is raised.

The Google AppEngine SDK's dev_appserver.py must be available on PATH.

To use this module as a library:

  import gae_dashboard_curl
  dashclient = gae_dashboard_curl.DashboardClient(email, password)
  instances_html = dashclient.fetch('/instances?app_id=s~test-app')

"""

import os
import sys

# Set up GAE import paths via gae_util.py in src/
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
import gae_util
gae_util.fix_sys_path()

from google.appengine.tools import appengine_rpc

APPENGINE_HOST = 'appengine.google.com'
AUTH_SOURCE = 'gae_dashboard_curl-1.0'
USER_AGENT = 'gae_dashboard_curl.py/1.0'


class UnsupportedUrlError(Exception):
    """Raised when given an URL that is not an App Engine dashboard."""
    pass


class DashboardClient(object):
    """Fetch URLs in the AppEngine admin interface."""
    def __init__(self, email, password):
        self.rpcserver = create_rpcserver(email, password)

    def fetch(self, url):
        return fetch_contents(self.rpcserver, url)


def create_rpcserver(email, password):
    """Create an instance of an RPC server to access GAE dashboard pages."""

    # Executing "appcfg.py update ." results in the following
    # arguments to appengine_rpc.HttpRpcServer.__init__():
    #
    # *args    ('appengine.google.com',
    #           <function GetUserCredentials at 0x10ec58320>,
    #           'appcfg_py/1.7.2 Darwin/11.4.0 Python/2.7.1.final.0',
    #           'Google-appcfg-1.7.2')
    #
    # **kwargs {'host_override': None,
    #           'auth_tries': 3,
    #           'save_cookies': True,
    #           'account_type': 'HOSTED_OR_GOOGLE',
    #           'secure': True}
    rpcserver = appengine_rpc.HttpRpcServer(
        APPENGINE_HOST,
        lambda: (email, password),
        USER_AGENT,
        AUTH_SOURCE,
        host_override=None,
        auth_tries=1,
        save_cookies=False,
        account_type='HOSTED_OR_GOOGLE',
        secure=True,
        rpc_tries=3)
    return rpcserver


def fetch_contents(rpcserver, url):
    """Fetch a URL from the AppEngine admin interface."""

    # Determine the request path. It's OK if this has a query string.
    valid_host_prefix = 'https://%s' % APPENGINE_HOST
    if url.startswith('/'):
        # Treat input as a server-relative path on APPENGINE_HOST.
        request_path = url
    elif url.startswith(valid_host_prefix):
        request_path = url[len(valid_host_prefix):]
    else:
        raise UnsupportedUrlError(
            'URL to fetch must start with / or %s/. Saw %s' %
            (valid_host_prefix, url))

    return rpcserver.Send(request_path, None)


def main():
    if len(sys.argv) != 3:
        print >>sys.stderr, (
            'Usage: echo GAE_PASSWORD | gae_dashboard_curl.py URL GAE_USER')
        return 1
    _, url, email = sys.argv
    password = sys.stdin.read().rstrip('\n')
    dashclient = DashboardClient(email, password)
    print dashclient.fetch(url)
    return 0


if __name__ == '__main__':
    exit_status = min(main(), 127)  # 128+ is for signals.
    sys.exit(exit_status)
