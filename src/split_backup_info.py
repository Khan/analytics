#!/usr/bin/python

"""Split a per-kind .backup_info to fit under BigQuery's 1Ti limit.

This naively, evenly, splits the data files references by a per-kind
.backup_info file (e.g., ProblemLog.backup_info) into two files:
ProblemLog_1of2.backup_info and ProblemLog_2of2.backup_info

It's a good enough solution for ProblemLog, our largest dataset, so
it's good enough for now.

USAGE

First, run a backup of a large table (total data >1TB in backup
format) from the AppEngine Datastore Admin panel. Then download the
table's per-kind .backup_info file from Google Cloud Storage and split
it into multiple per-kind .backup_info files.

The real filename will look something like
ag5zfmtoYW4tYWNhZGVteXJCCxIcX0FFX0RhdGFzdG9yZUFkbWluX09wZXJhdGlvbhik8bPgBgwLEhZfQUVfQmFja3VwX0luZm9ybWF0aW9uGAEM.ProblemLog.backup_info @Nolint

$ ./split_backup_info.py ProblemLog.backup_info
Wrote ProblemLog_1of2.backup_info
Wrote ProblemLog_2of2.backup_info

Next, upload the new files to Google Cloud Storage. Finally, run two
import jobs in BigQuery, one for each of the new per-kind .backup_info
files. There are details at this link for how to ingest an AppEngine
backup into BigQuery but you need to be signed up for the "Trusted
Tester" program to read them:
https://developers.google.com/bigquery/loading-data-into-bigquery#appenginedatastore

TODO(chris): instead of naively splitting in half we could be smarter
and first determine the size of the referenced data and put as much in
the first split as possible.

"""

import optparse
import os
import sys

import gae_util
gae_util.fix_sys_path()

from google.appengine.ext.datastore_admin import backup_pb2


def main(backup_info, force_overwrite=False):
    """Split up the per-kind .backup_info file.

    Arguments:
      backup_info - filename of the per-kind .backup_info file, e.g.,
          <some hash string>.ProblemLog.backup_info
      force_overwrite - if true, overwrite existing files.

    Returns a string or integer value to pass to sys.exit().
    """
    # Make new filenames for the splits.
    if not backup_info.endswith(".backup_info"):
        raise ValueError("Backup filename must end in .backup_info")
    split_1 = "%s_1of2.backup_info" % backup_info[:-len(".backup_info")]
    split_2 = "%s_2of2.backup_info" % backup_info[:-len(".backup_info")]

    if not force_overwrite and os.path.exists(split_1):
        return "%s exists. Refusing to overwrite" % split_1
    if not force_overwrite and os.path.exists(split_2):
        return "%s exists. Refusing to overwrite" % split_2

    # For reading/writing per-kind .backup_info, see
    # backup_handler.BackupInfoWriter._write_kind_backup_info_file in
    # google.appengine.ext.datastore_admin (as of SDK 1.8.0).
    with open(backup_info, "rb") as f:
        backup = backup_pb2.Backup()
        backup.ParseFromString(f.read())

    # Create the splits from the original .backup_info.
    assert len(backup.kind_info) == 1, "Can only handle one kind_info"
    files = list(backup.kind_info[0].file)  # copy or be confused!
    with open(split_1, "wb") as f:
        del backup.kind_info[0].file[:]
        backup.kind_info[0].file.extend(files[:len(files) / 2])
        f.write(backup.SerializeToString())
        print >>sys.stderr, "Wrote %s" % split_1
    with open(split_2, "wb") as f:
        del backup.kind_info[0].file[:]
        backup.kind_info[0].file.extend(files[len(files) / 2:])
        f.write(backup.SerializeToString())
        print >>sys.stderr, "Wrote %s" % split_2

    return 0


if __name__ == "__main__":
    parser = optparse.OptionParser(usage="usage: %prog [options]"
                                         " <KIND_NAME>.backup_info")
    parser.add_option("-f", "--force", action="store_true", default=False,
                      help="overwrite existing Kind_N.backup_info files."
                           " Default is to fail immediately")
    (options, args) = parser.parse_args()
    if len(args) != 1:
        parser.error("missing required .backup_info file")
    backup_info = args[0]
    sys.exit(main(backup_info, options.force))
