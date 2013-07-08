"""Miscellaneous utilities for interacting with GAE."""

import os
import sys


def _discover_sdk_path():
    """Return directory from $PATH where the Google Appengine DSK lives."""
    # adapted from {http://code.google.com/p/bcannon/source/browse/
    # sites/py3ksupport-hrd/run_tests.py}

    # Poor-man's `which` command.
    for path in os.environ['PATH'].split(':'):
        if os.path.isdir(path) and 'dev_appserver.py' in os.listdir(path):
            # Follow symlinks to the real appengine install directory.
            realpath = os.path.realpath(os.path.join(path, 'dev_appserver.py'))
            path = os.path.dirname(realpath)
            break
    else:
        raise RuntimeError("couldn't find dev_appserver.py on $PATH")

    # Verify the App Engine installation directory looks right.
    assert os.path.isdir(os.path.join(path, 'google', 'appengine')), path
    return path


def fix_sys_path(appengine_sdk_dir=None):
    """Update sys.path for appengine khan academy imports, also envvars."""

    if 'SERVER_SOFTWARE' not in os.environ:
        os.environ['SERVER_SOFTWARE'] = 'Development'
    if 'CURRENT_VERSION' not in os.environ:
        os.environ['CURRENT_VERSION'] = '764.1'

    if not appengine_sdk_dir:
        appengine_sdk_dir = _discover_sdk_path()
    sys.path.append(appengine_sdk_dir)

    # Delegate the real work to the dev_appserver logic in the actual SDK.
    import dev_appserver
    dev_appserver.fix_sys_path()

