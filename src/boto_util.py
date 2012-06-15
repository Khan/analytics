"""Utilities for extending boto.

See https://github.com/boto/boto for detals.
"""

import os


def initialize_creds_from_file(creds_filename=None):
    """Sets up environment variables boto relies on for auth keys.

    This reads the credentials from a file specified. By default, it looks
    in $HOME/.botorc for a file with two lines: one line for the
    AWS_ACCESS_KEY_ID and the second line being the AWS_SECRET_ACCESS_KEY.

    Will raise an exception if no file is found.
    """
    if creds_filename is None:
        home = os.environ.get('HOME', '')
        creds_filename = os.path.join(home, '.botorc')

    if not os.path.exists(creds_filename):
        raise Exception("No credential file found for AWS access via Boto. "
                        "(Looked in '%s')" % creds_filename)

    with open(creds_filename) as f:
        access_key_id = f.readline().rstrip()
        secret = f.readline().rstrip()    
        initialize_creds(access_key_id, secret)


def initialize_creds(access_key_id, secret):
    """Sets up environment variables boto relies on for auth keys."""
    os.environ['AWS_ACCESS_KEY_ID'] = access_key_id
    os.environ['AWS_SECRET_ACCESS_KEY'] = secret

