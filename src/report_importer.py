#!/usr/bin/env python

"""Tools for importing data from the Hive data warehouse to reporting db's.

This reads raw table data from S3 and puts the data into a mongo db.
Typically, the summarization of reporting data is done in Hive queries or
a map reduce, but emitted to summary tables that are of manageable size by
mongo. This simply helps import that data for mongo for reporting dashboards.
"""


USAGE = """%prog [options] [table_loc] \\
           [target_db] [target_collection] [col_0] [col_1] ...

Reads lines from files representing Hive table data on S3 and dumps them to
a reporting database.

The table location must be a string indicating the path
of the table in S3 in our default bucket (i.e. the
part after s3://ka-mapreduce/ in the full S3 path).

Each row will be saved to the reporting table as a document in the specified
target_collection.
"""

import sys

import boto
import pymongo
import optparse

import boto_util


def main(table_location, target_db, target_collection, col_names, options):

    boto_util.initialize_creds_from_file()

    # Default row format for the tables are delimited by this control char.
    # when stored on disk.
    delimiter = '\01'
    key_index = options.key_index
    num_cols = len(col_names)

    if key_index >= num_cols:
        raise Exception("Invalid key index (there aren't enough columns)")

    # Open our target db connection
    mongo_conn = pymongo.Connection(options.report_db_host,
                                    port=options.report_db_port)
    mongodb = mongo_conn[target_db]

    # Open our input connections.
    s3conn = boto.connect_s3()
    bucket = s3conn.get_bucket('ka-mapreduce')
    s3keys = bucket.list(prefix=table_location)
    saved = 0

    # Note: a table's data may be broken down into multiple files on disk.
    for key in s3keys:
        print "Opening S3 file for read: [%s]" % key
        contents = key.get_contents_as_string()

        for line in contents.split('\n'):
            if not line:
                # EOF
                break

            parts = line.strip().split(delimiter)
            if len(parts) != num_cols:
                # TODO(benkomalo): properly handle? shouldn't happen though.
                sys.stderr.write(
                    "Unexpected number of columns in row (expected [%s]):\n" %
                     num_cols)
                print >> sys.stderr, parts
                continue

            doc = dict(zip(col_names, parts))
            if key_index > -1:
                # mongo primary keys are labelled as "_id"
                doc['_id'] = parts[key_index]

            mongodb[target_collection].save(doc)
            saved += 1

    mongo_path = "%s:%s/%s/%s" % (
            options.report_db_host,
            options.report_db_port or '[default_mongo_port]',
            target_db,
            target_collection)
    print "Succesfully saved [%s] documents to '%s'" % (saved, mongo_path)


if __name__ == '__main__':
    parser = optparse.OptionParser(USAGE)
    parser.add_option('--key_index', type="int",
                      default=-1,
                      help=('The column index to become the key in the '
                            'reporting value. If not specified, the reporting '
                            'table will auto-generate ids (but note then that '
                            'you may have duplicate data if you re-import)'))
    parser.add_option('--report_db_host',
                      default='184.73.72.110',
                      help='The hostname of the reporting db')
    parser.add_option('--report_db_port',
                      default=None,
                      help='The port of the reporting db')
    
    options, args = parser.parse_args()

    if len(args) < 3:
        print >> sys.stderr, USAGE
        sys.exit(-1)

    table_location = args[0]
    target_db = args[1]
    target_collection = args[2]
    col_names = args[3:]
    if not col_names:
        raise Exception("Must specify column names")

    main(table_location, target_db, target_collection, col_names, options)
