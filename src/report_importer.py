#!/usr/bin/env python

"""Tools for importing data from the Hive data warehouse to reporting db's.

This reads raw table data from S3 and puts the data into a mongo db.
Typically, the summarization of reporting data is done in Hive queries or
a map reduce, but emitted to summary tables that are of manageable size by
mongo. This simply helps import that data for mongo for reporting dashboards.
"""


USAGE = """%prog [options] \\
            [hive_masternode] [table_name] \\
            [target_db] [target_collection] \\
            [[partition_col_0] [partition_col_1] ...]

Reads lines from files representing Hive table data on S3 and dumps them to
a reporting database. The data is read off of S3, but requires meta data to
first be read about it from a Hive masternode.

If tables are partitioned, partition columns will be included in the imported
data. Clients may specify partition columns using a key=value
format, in which case only the data in partitions with those matching columns
will be imported. If no partition columns are specified, this will import all
partitions (so you may want to delete data in the report db first, else you
may get duplicates)

TODO(benkomalo): write out partitions imported to a meta table or something
   so the import process is idempotent

Each row will be saved to the reporting table as a document in the specified
target_collection.
"""

import datetime
import optparse
import sys

import boto
import pymongo

import boto_util
import hive_mysql_connector


def main(table_location,
         target_db,
         target_collection,
         column_info,
         partition_cols,
         options):

    boto_util.initialize_creds_from_file()

    # TODO(benkomalo): handle other formats? It may also be compressed, so
    #    we may have to do more work.
    # Default row format for the tables are delimited by this control char.
    # when stored on disk.
    delimiter = '\01'
    key_index = options.key_index
    num_cols = len(column_info)
    col_names, col_types, _ = zip(*column_info)

    if key_index >= num_cols:
        raise Exception("Invalid key index (there aren't enough columns)")

    base_partition_values = {}
    for partition in partition_cols:
        key, value = partition.split('=')
        base_partition_values[key] = value
    table_location += _format_path_from_partition(partition_cols)

    # Open our target db connection
    mongo_conn = pymongo.Connection(options.report_db_host,
                                    port=options.report_db_port)
    mongodb = mongo_conn[target_db]
    mongo_collection = mongodb[target_collection]

    # If drop flag was set, nuke any pre-existing data
    if options.drop:
        print "\nDropping existing data in collection %s." % target_collection
        # Even though the option is called 'drop', I am instead
        # calling 'remove', because it leaves any indexes while deleting data.
        mongo_collection.remove()
    else if options.drop_partition:
        print ("\nDropping existing data in collection {0} "
                "on partition spec {1}".format(
                    target_collection, partition_cols)

        mongo_collection.remove(base_partition_values)

    # Open our input connections.
    s3conn = boto.connect_s3()
    bucket = s3conn.get_bucket('ka-mapreduce')
    path_prefix = table_location[len('s3://ka-mapreduce/'):] + '/'
    s3keys = bucket.list(prefix=path_prefix)

    # TODO(benkomalo): add a flag to bail on any errors so no partial data is
    #    saved?
    # Counts of rows saved, and rows with errors.
    saved = 0
    errors = 0
    NULL_STRING = '\N'

    docs_to_insert = []
    batch_size = max(1, options.batch_size)

    lines_parsed = 0

    # Note: a table's data may be broken down into multiple files on disk.
    for key in s3keys:
        if key.name.endswith('_$folder$'):
            # S3 meta data - not useful.
            continue

        # Find out if files are under additional partition "directories"
        # e.g. "start_dt=2012-01-01/end_dt=2012-02-01/some_file"
        base_path = key.name[len(path_prefix):]
        path_parts = base_path.split('/')
        partition_values = base_partition_values.copy()
        for partition in path_parts[:-1]:
            name, value = partition.split('=')
            partition_values[name] = value

        contents = key.get_contents_as_string()
        for line in contents.split('\n'):

            if not line:
                # EOF
                break

            # HACK: some files are '\t' delimited? Switch delimiters.
            # TODO(benkomalo): read format from metadata in Hive master?
            if delimiter not in line:
                delimiter = '\t'

            parts = line.strip().split(delimiter)
            if len(parts) != num_cols:
                # TODO(benkomalo): properly handle? shouldn't happen though.
                sys.stderr.write(
                    "Unexpected number of columns in row (expected [%s]):\n" %
                     num_cols)
                print >> sys.stderr, parts
                continue

            doc = {}
            for i, (name, type) in enumerate(zip(col_names, col_types)):
                # TODO(benkomalo): deal with other types and possible UTF-8
                #    issues?
                try:
                    if type == 'int':
                        value = int(parts[i])
                    elif type == 'boolean':
                        value = parts[i] == 'true'
                    elif type in ['double', 'float']:
                        value = float(parts[i])
                    else:
                        value = parts[i]
                except Exception:
                    if parts[i] == NULL_STRING:
                        # TODO(benkomalo): figure out why our data has this.
                        # It seems that sometimes Hive likes to put in
                        # NULL values for ints and booleans? They don't parse
                        # well. This is unfortunate - just skip the row since
                        # it's pretty rare for now.
                        doc = None
                        break
                    raise

                doc[name] = value

            if key_index > -1:
                # mongo primary keys are labelled as "_id"
                doc['_id'] = parts[key_index]

            if doc:

                doc.update(partition_values)

                # If we have doc IDs, perform a single upsert because bulk
                # upserts are not currently supported by pymongo according to
                # this old answer: http://stackoverflow.com/questions/5292370
                if '_id' in doc:
                    mongo_collection.save(doc)
                    saved += 1
                else:
                    docs_to_insert.append(doc)

            else:
                errors += 1

            # Bulk insert docs in batches
            if len(docs_to_insert) >= batch_size:
                mongo_collection.insert(docs_to_insert)
                saved += len(docs_to_insert)
                docs_to_insert = []

            if lines_parsed % 100 == 0:
                print "\rSaved %s docs with %s errors..." % (saved, errors),
                sys.stdout.flush()

            lines_parsed += 1

    if docs_to_insert:
        mongo_collection.insert(docs_to_insert)
        saved += len(docs_to_insert)

    print "\nSummary of results:"
    print "\tSaved [%s] documents" % saved
    print "\tSkipped [%s] documents with errors" % errors


def parse_command_line_args():
    parser = optparse.OptionParser(USAGE)
    parser.add_option('--key_index', type="int",
        default=-1,
        help=('The column index to become the key in the '
              'reporting value. If not specified, the reporting '
              'table will auto-generate ids (but note then that '
              'you may have duplicate data if you re-import)'))
    # TODO(benkomalo): add in a flag to delete existing data.
    parser.add_option('--report_db_host',
        default='107.21.23.204',
        help='The hostname of the reporting db')
    parser.add_option('--report_db_port',
        default=None,
        help='The port of the reporting db')
    parser.add_option('--ssh_keyfile',
        help=('A location of an SSH pem file to use for SSH connections '
              'to the specified Hive machine'))
    parser.add_option('--batch_size', type="int", default=1000,
        help=('Number of documents to insert in a single database call. '
              'Will only do bulk inserts if no key index is specified, '
              'because pymongo does not currently support bulk upserts.'))
    parser.add_option("--drop", action="store_true",
        dest="drop", default=False,
        help=('This flag will delete the entire target collection before '
              'inserting the new data.  Use with caution!'))
    parser.add_option("--drop_partition", action="store_true",
        dest="drop_partition", default=False,
        help=('This flag will delete the all rows in the target collection '
              'that match the value(s) passed in for the partition(s) before '
              'inserting the new data.  Use with caution!'))
    parser.add_option("--hive_init", action="store_true",
        dest="hive_init", default=False,
        help=('If True, this script will execute ka_hive_init.q on the '
              'hive_masternode before attempting the import.  This is useful '
              'for making sure the metadata on the hive cluster is up to '
              'date, e.g. if data you want to import was create by an '
              'unknown on-demand cluster.'))

    options, args = parser.parse_args()
    if len(args) < 4:
        print >> sys.stderr, USAGE
        sys.exit(-1)

    return options, args


def _format_path_from_partition(partition_cols):
    """Build out the S3 suffix for a partition.

    Arguments:
        partition_cols - A list of strings in 'key=value' format for partition
    """
    return ''.join(['/%s' % p for p in partition_cols])


def print_locations(table_location, column_info, partition_cols,
                    target_db, target_collection, options):
    print "Input:"
    print "\t%s%s" % (table_location,
                      _format_path_from_partition(partition_cols))
    print "Columns:"
    for name, type, comment in column_info:
        print "\t%s (%s)\t%s" % (name,
                                 type,
                                 comment if comment != 'NULL' else '')

    mongo_path = "%s:%s/%s/%s" % (
            options.report_db_host,
            options.report_db_port or '[default_mongo_port]',
            target_db,
            target_collection)
    print "\nOutput:"
    print "\t%s" % mongo_path


if __name__ == '__main__':
    start_dt = datetime.datetime.now()

    # Named arguments appear as properties on the options object
    # Unnamed arguments appears as elements in the args array
    options, args = parse_command_line_args()

    # Step 1 - read meta data.
    hive_masternode = args[0]  # Generally, 'ka-hive'
    hive_mysql_connector.configure(hive_masternode, options.ssh_keyfile)

    if options.hive_init:
        hive_mysql_connector.run_hive_init()

    table_name = args[1]  # Generally == target_collection = args[3]
    print "Fetching table info..."
    table_location = hive_mysql_connector.get_table_location(table_name)

    if not table_location:
        raise Exception("Can't read info about %s in Hive master %s" %
                        (hive_masternode, table_name))
    if not table_location.startswith('s3://ka-mapreduce/'):
        raise Exception("Can only import from s3://ka-mapreduce for now")
    column_info = hive_mysql_connector.get_table_columns(table_name)

    target_db = args[2]  # Always 'report'
    target_collection = args[3]  # Generally == table_name = args[1]
    partition_cols = args[4:]  # Something like ['dt=2013-05', user='hello']

    # TODO(benkomalo): prompt/dry-run flags?
    # Step 2 - print locations
    print_locations(table_location, column_info, partition_cols,
                    target_db, target_collection, options)

    # Step 3 - read the data!
    main(table_location,
         target_db,
         target_collection,
         column_info,
         partition_cols,
         options)

    time_taken = datetime.datetime.now() - start_dt
    print "\nTotal wall time taken: %s seconds" % time_taken.total_seconds()
