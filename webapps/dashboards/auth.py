"""Authentication utilities for the dashboard, including decorators.

Example usage:
    app = flask.Flask(__name__)
    auth.configure_app(app)

    @app.route('/secret-page')
    @auth.login_required
    def secret_page():
        pass
"""

import datetime
from functools import wraps
import json
import logging
import urllib2

import flask
from oauth import OAuth

try:
    import secrets
    GOOGLE_CLIENT_ID = secrets.GOOGLE_CLIENT_ID
    GOOGLE_CLIENT_SECRET = secrets.GOOGLE_CLIENT_SECRET
    SECRET_KEY = secrets.SECRET_KEY
    FAKED_SECRETS = False
except ImportError:
    logging.critical("Unable to find secrets.py. Cannot force authentication")
    GOOGLE_CLIENT_ID = 'dummy google client id'
    GOOGLE_CLIENT_SECRET = 'dummy google client secret'
    SECRET_KEY = 'dummy secret key'
    FAKED_SECRETS = True


REDIRECT_URI = '/oauth2callback'


_auth_whitelist = set()
try:
    import auth_whitelist
    _auth_whitelist = set(auth_whitelist.auth_whitelist)
except ImportError:
    # Auth whitelist is optional. No biggie if it's not there.
    pass


oauth = OAuth()
google = oauth.remote_app(
        'google',
        base_url='https://www.google.com/accounts/',
        authorize_url='https://accounts.google.com/o/oauth2/auth',
        request_token_url=None,
        request_token_params={
            'scope': 'https://www.googleapis.com/auth/userinfo.email',
            'response_type': 'code'
        },
        access_token_params={'grant_type': 'authorization_code'},
        access_token_url='https://accounts.google.com/o/oauth2/token',
        access_token_method='POST',
        consumer_key=GOOGLE_CLIENT_ID,
        consumer_secret=GOOGLE_CLIENT_SECRET,
        )


def configure_app(app, required=True):
    """Configures the given flask app to handle Google authentication."""

    if required and FAKED_SECRETS:
        raise Exception("No secrets.py file detected, but authentication is "
                        "marked as required. Please get a secrets.py file.")

    app.secret_key = SECRET_KEY
    app.add_url_rule('/login', 'login', login)
    app.add_url_rule(REDIRECT_URI, 'authorized', authorized)


def login():
    # TODO(benkomalo): pass along continue url.
    callback = flask.url_for('authorized', _external=True)

    continue_url = flask.request.args.get('continue', '/')

    # Store continue_url in 'state' param
    # TODO(alpert): This is horrible and not threadsafe
    google.request_token_params['state'] = continue_url
    resp = google.authorize(callback=callback)
    del google.request_token_params['state']

    return resp


@google.authorized_handler
def authorized(resp):
    access_token = resp['access_token']
    flask.session['access_token'] = access_token, ''
    flask.session.permanent = True

    # continue_url is stored in 'state' param
    continue_url = flask.request.args.get('state', '/')
    return flask.redirect(continue_url)


@google.tokengetter
def get_access_token():
    return flask.session.get('access_token')


def login_required(func):
    @wraps(func)
    def auth_wrapper(*args, **kwargs):
        if FAKED_SECRETS:
            return func(*args, **kwargs)

        login_url = flask.url_for('login', **{
            # continue is reserved, so we do this funny kwarg dance
            'continue': flask.request.path})

        # TODO(benkomalo): pass along continue url.
        access_token = flask.session.get('access_token')
        if access_token is None:
            # TODO(benkomalo): add in a flag here just in case something is
            # wrong with cookie setting and we get into an infinite redirect.
            return flask.redirect(login_url)

        access_token = access_token[0]
        email = _get_verified_user(access_token)
        if not email:
            # Couldn't get user info - the access_token must have expired
            # or is invalid.
            return flask.redirect(login_url)
        elif (email in _auth_whitelist or
              email.lower().endswith('@khanacademy.org')):
            return func(*args, **kwargs)
        else:
            return ("Unauthorized. Logged in as %s, but this requires "
                    "a @khanacademy.org account.\n"
                    "TODO(benkomalo): provide option to retry. Right now you "
                    "probably have to clear your cookies and try again if "
                    "you accidentally used the wrong account." % email)
    return auth_wrapper


# TODO(benkomalo): replace this hacky cache with a real one.
# access_token -> (email, expiry)
_TOKEN_CACHE = {}
_CACHE_EXPIRY = datetime.timedelta(minutes=5)


def _get_verified_user(access_token):
    if access_token in _TOKEN_CACHE:
        email, expiry = _TOKEN_CACHE[access_token]
        if expiry <= datetime.datetime.utcnow():
            # Expired. Purge from cache
            del _TOKEN_CACHE[access_token]
        else:
            return email

    headers = {'Authorization': 'OAuth ' + access_token}
    req = urllib2.Request('https://www.googleapis.com/oauth2/v1/userinfo',
                          None,
                          headers)
    try:
        res = urllib2.urlopen(req)
    except urllib2.URLError, e:
        logging.error("Can't reach Google to authenticate. [%s]" % e)
        return None

    resp = json.loads(res.read())
    email = resp.get('email', '')
    verified = resp.get('verified_email', False)
    if verified:
        expiry = datetime.datetime.utcnow() + _CACHE_EXPIRY
        _TOKEN_CACHE[access_token] = (email, expiry)
        return email
    return None

