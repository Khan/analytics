#!/usr/bin/env python
"""Deploy utilities for map reduce code.

This is a simple wrapper around copying files to S3 for the most part.
"""

import optparse
import os
import subprocess
import sys

import boto

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))
import boto_util
import notify

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


def files_in_tree():
    """Collects names of all candidate files for deployment to S3."""
    files = []
    extensions_allowed = ['py', 'q']

    unwanted_prefix = '.' + os.path.sep
    for (dirpath, dirnames, filenames) in os.walk('.'):
        for filename in filenames:
            if filename == os.path.basename(__file__):
                continue

            for ext in extensions_allowed:
                if filename.endswith('.' + ext):
                    path = os.path.join(dirpath, filename)
                    if path.startswith(unwanted_prefix):
                        path = path[len(unwanted_prefix):]
                    files.append(path)
    return files


def _get_branch_dirname(branch=""):
    dirname = 'code/'
    if branch:
        if branch.endswith('/'):
            dirname += branch
        else:
            dirname += branch + '/'
    return dirname


def files_in_prod(branch=""):
    """Collects names of code files in ka-mapreduce S3 code bucket."""

    s3conn = boto.connect_s3()
    bucket = s3conn.get_bucket('ka-mapreduce')
    extensions_allowed = ['py', 'q']

    dirname = _get_branch_dirname(branch)

    files = []
    for key in bucket.list(prefix=dirname):
        name = key.name[len(dirname):]
        # TODO(benkomalo): Maybe we should have code/prod/ and code/dev.
        # As it is, "prod" is just under "ka-mapreduce/code/" and "dev" is
        # under "ka-mapreduce/code/dev", but we may have other branches
        # So it'll be weird with prod being in the top level, and other
        # branches underneath.

        if not branch and (name.startswith('dev/') or
                           name.startswith('branch-')):
            # Skip dev or branch subdirs - we don't care about
            # them in the deploy process of the top level main branch.
            continue
        if not any(name.endswith('.' + ext) for ext in extensions_allowed):
            # Don't care about unknown files.
            continue
        files.append(str(name))

    return files


def copy_files_to_prod(filenames, branch=None):
    """Copies all given files to ka-mapreduce S3 code bucket"""

    dirname = _get_branch_dirname(branch)

    s3conn = boto.connect_s3()
    bucket = s3conn.get_bucket('ka-mapreduce')
    for filename in filenames:
        with open(filename, 'r') as filestream:
            filepath = "%s%s" % (dirname, filename)
            key = bucket.get_key(filepath)
            if not key:
                key = bucket.new_key(filepath)
            key.set_contents_from_file(filestream)


def summarize_changes(replaced_files, new_files, spurious_files):
    args = {
        'num_replaced': len(replaced_files),
        'num_new': len(new_files),
    }

    # TODO(benkomalo): do some kind of timestamp checking
    # TODO(benkomalo): put in the git version in some kind of file in S3 so
    #                  we can do version tracking of what's deployed and do
    #                  a delta with names of authors of all changes since
    #                  last deploy
    spurious_files_warning = ""
    if spurious_files:
        spurious_files_warning = (
            "(%d files on S3 but not in codebase)" % len(spurious_files))

    return (("%(num_replaced)d updated files, %(num_new)d new files. " +
             spurious_files_warning +
             " (note - no timestamp checking is done. files are always " +
             "pushed if they exist in the local tree)")
             % args)


def send_hipchat_deploy_message(
        replaced_files, new_files, spurious_files, dest_path):
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
        'dest_path': dest_path,
        'summary': summarize_changes(replaced_files,
                                     new_files,
                                     spurious_files),
    }
    message = ("%(deployer)s just deployed files to S3.<br>" +
               "Destination: %(dest_path)s<br>" +
               "Version: <a href='%(github_url)s'>%(version_stamp)s</a> " +
               "- %(git_msg)s<br>" +
               "%(summary)s") % args

    notify.send_hipchat(message, ["analytics"])


# TODO(benkomalo): wire up options for subdirectory to deploy to (for testing)
def do_deploy(verbose, branch=""):
    in_tree = set(files_in_tree())
    in_prod = set(files_in_prod(branch))
    new_files = in_tree - in_prod
    replaced_files = in_prod & in_tree
    spurious_files = in_prod - in_tree

    if not replaced_files and not new_files:
        print "No files to update and no new files"
        return

    files_to_push = new_files | replaced_files

    dest_path = "s3://ka-mapreduce/code/%s" % branch
    print "About to deploy to [%s] with the following changes:" % dest_path
    print summarize_changes(replaced_files, new_files, spurious_files)
    if verbose and spurious_files:
        print "\nFiles in %s not in local tree:" % dest_path
        print "\n".join("\t%s" % filename for filename in spurious_files)
    if raw_input("Proceed? [y/N]: ").lower() not in ['y', 'yes']:
        return

    copy_files_to_prod(files_to_push, branch)
    print "Done!"
    if not branch.startswith("branch-"):  
        # we only notify if not pushing to a personal branch.
        # "branch-" is the conventional prefix for a personal branch. 
        send_hipchat_deploy_message(
                replaced_files, new_files, spurious_files, dest_path)


if __name__ == '__main__':
    parser = optparse.OptionParser()
    parser.add_option('-v', '--verbose',
        action="store_true", dest="verbose",
        help="Print more information during the deploy process")
    parser.add_option('-b', '--branch',
        default="",
        help=("The branch to deploy to. By default, no branch is specified "
              "implying that the default production branch is used"))

    options, args = parser.parse_args()
    do_deploy(options.verbose, options.branch)
