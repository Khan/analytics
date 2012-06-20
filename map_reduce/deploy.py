#!/usr/bin/env python
"""Deploy utilities for map reduce code.

This is a simple wrapper around copying files to S3 for the most part.
"""

import optparse
import os
import subprocess
import sys

import boto
import hipchat.room
import hipchat.config

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))
import boto_util


hipchat.config.init_cfg('hipchat.cfg')


if not hipchat.config.token:
    print >> sys.stderr, (
        'Can\'t find HipChat token. Make a hipchat.cfg file ' +
        'with a single line "token = <token_value>" ' +
        '(don\'t forget to chmod 600) either in this directory ' +
        'or in your $HOME directory')
    sys.exit(-1)
boto_util.initialize_creds_from_file()


def popen_results(args):
    proc = subprocess.Popen(args, stdout=subprocess.PIPE)
    return proc.communicate()[0]


def parse_git_version():
    return popen_results(['git', 'rev-parse', 'HEAD']).strip()


def parse_git_message():
    return popen_results(['git', 'show', '-s', '--pretty=format:%s']).strip()


def is_git_dirty():
    return len(popen_results(['git', 'status', '--short']).strip()) > 0


def _hipchat_message(msg, rooms):
    for room in hipchat.room.Room.list():
        if room.name in rooms:
            result = ""
            msg_dict = {
                "room_id": room.room_id,
                "from": "Mr Monkey",
                "message": msg,
                "color": "purple",
            }

            try:
                result = str(hipchat.room.Room.message(**msg_dict))
            except:
                pass

            if "sent" in result:
                print "Notified Hipchat room %s" % room.name
            else:
                print "Failed to send message to Hipchat: %s" % msg


def files_in_tree():
    """Collects names of all candidate files for deployment to S3."""
    files = []
    extensions_allowed = ['py', 'q']

    unwanted_prefix = '.' + os.path.sep
    for (dirpath, dirnames, filenames) in os.walk('.'):
        for filename in filenames:
            if filename == __file__:
                continue

            for ext in extensions_allowed:
                if filename.endswith('.' + ext):
                    path = os.path.join(dirpath, filename)
                    if path.startswith(unwanted_prefix):
                        path = path[len(unwanted_prefix):]
                    files.append(path)
    return files


def files_in_prod():
    """Collects names of code files in ka-mapreduce S3 code bucket."""

    s3conn = boto.connect_s3()
    bucket = s3conn.get_bucket('ka-mapreduce')
    extensions_allowed = ['py', 'q']

    dirname = 'code/'
    files = []
    for key in bucket.list(prefix=dirname):
        name = key.name[len(dirname):]
        if name.startswith('dev/'):
            # Skip dev files - we don't care about them in the deploy process
            continue
        if not any(name.endswith('.' + ext) for ext in extensions_allowed):
            # Don't care about unknown files.
            continue
        files.append(str(name))

    return files


def copy_files_to_prod(filenames, subdir=None):
    """Copies all given files to ka-mapreduce S3 code bucket"""

    dirname = 'code/'
    if subdir:
        dirname += subdir

    s3conn = boto.connect_s3()
    bucket = s3conn.get_bucket('ka-mapreduce')
    for filename in filenames:
        with open(filename, 'r') as filestream:
            filepath = "%s%s" % (dirname, filename)
            key = bucket.get_key(filepath)
            key = True
            if not key:
                key = bucket.new_key(filepath)
            key.set_contents_from_file(filestream)


def summarize_changes(replaced_files, new_files, spurious_files):
    args = {
        'num_replaced': len(replaced_files),
        'num_new': len(new_files),
        'num_unknown': len(spurious_files),
    }

    # TODO(benkomalo): do some kind of timestamp checking
    # TODO(benkomalo): put in the git version in some kind of file in S3 so
    #                  we can do version tracking of what's deployed and do
    #                  a delta with names of authors of all changes since
    #                  last deploy
    return ("%(num_replaced)d updated files, %(num_new)d new files. " +
            "(%(num_unknown)d files on S3 but not in codebase)" +
            " (note - no timestamp checking is done. files are always " +
            "pushed if they exist in the local tree)") % args


def send_hipchat_deploy_message(replaced_files, new_files, spurious_files):
    """Send a summary of the deploy information to HipChat."""

    git_version = parse_git_version()
    git_msg = parse_git_message()

    git_version_stamp = git_version
    includes_local_changes = is_git_dirty()
    if includes_local_changes:
        git_version_stamp = "%s+ (including local changes)" % git_version

    github_url = "https://github.com/Khan/analytics/commit/%s" % git_version

    deployer_id = popen_results(['whoami']).strip()

    args = {
        'deployer': deployer_id,
        'num_files': 0,
        'version_stamp': git_version_stamp,
        'git_msg': git_msg,
        'github_url': github_url,
        'summary': summarize_changes(replaced_files,
                                     new_files,
                                     spurious_files),
    }
    message = ("%(deployer)s just deployed files to S3.<br>" +
               "Version: <a href='%(github_url)s'>%(version_stamp)s</a> " +
               "- %(git_msg)s<br>" +
               "%(summary)s") % args

    _hipchat_message(message, ["analytics"])
    print "Notified room analytics"


# TODO(benkomalo): wire up options for subdirectory to deploy to (for testing)
def do_deploy(verbose):
    in_tree = set(files_in_tree())
    in_prod = set(files_in_prod())
    new_files = in_tree - in_prod
    replaced_files = in_prod & in_tree
    spurious_files = in_prod - in_tree

    if not replaced_files and not new_files:
        print "No files to update and no new files"
        return

    files_to_push = new_files | replaced_files

    print "About to deploy with the following changes:"
    print summarize_changes(replaced_files, new_files, spurious_files)
    if verbose and spurious_files:
        print "\nFiles in S3 not in local tree:"
        print "\n".join("\t%s" % filename for filename in spurious_files)
    if raw_input("Proceed? [y/N]: ").lower() not in ['y', 'yes']:
        return

    copy_files_to_prod(files_to_push)
    print "Done!"
    send_hipchat_deploy_message(replaced_files, new_files, spurious_files)


if __name__ == '__main__':
    parser = optparse.OptionParser()
    parser.add_option('-v', '--verbose',
        action="store_true", dest="verbose",
        help="Print more information during the deploy process")

    options, args = parser.parse_args()
    do_deploy(options.verbose)
