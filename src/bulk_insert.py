#!/usr/bin/env python

""" This script will insert a bulkloader-downloaded data file into mongo. """

import gae_util
gae_util.fix_sys_path()
from google.appengine.datastore import entity_pb
from google.appengine.api import datastore

from optparse import OptionParser
import pymongo
import sqlite3

import gae_download
import util


def insert_dumpfile_into_mongo(filename, gae_download_config, mongo_conn, 
                               offset=0, limit=0):

    sqlite_conn = sqlite3.connect(filename, isolation_level=None)

    sqlstring = 'select id, value from result'
    if limit > 0:
        sqlstring += ' limit %d' % limit
    if offset > 0:
        sqlstring += ' offset %d' % offset

    cursor = sqlite_conn.cursor()
    cursor.execute(sqlstring)

    for unused_entity_id, entity in cursor:

        entity_proto = entity_pb.EntityProto(contents=entity)
        # TODO(jace): would be nice to avoid the private method call here,
        # but in order, e.g., to use the public model_from_protobuf()
        # function we would need to support a dependency on all the db.Model
        # subclasses.  
        entity = datastore.Entity._FromPb(entity_proto)

        gae_download.put_document(entity, gae_download_config, mongo_conn)
    

def get_cmd_line_args():

    parser = OptionParser(usage="%prog [options]", 
        description="Tool to load data from various sources into mongoDB.")
    parser.add_option("-f", "--file", 
        help="Data file to be read and inserted. Required argument.")
    parser.add_option("-c", "--config", 
        help="Config file to use as expected by gae_download.py.  Required.")
    parser.add_option("-s", "--server", 
        help="The mongoDB host to use", default="localhost")
    parser.add_option("-p", "--port", 
        help="The port to use for the mongoDB connection", default=27017)
    parser.add_option("-o", "--offset", 
        help="Offset for indexing into the dumpfile.  Default=0.", default=0)
    parser.add_option("-l", "--limit", 
        help="Limit of rows to load from the dumpfile. Default=0.", default=0)

    options, extra_args = parser.parse_args()

    if None in [options.file, options.config]:
        parser.print_help()
        exit(-1)

    return options

        
def main():
    options = get_cmd_line_args()
    # NOTE: the Mongo connection specified on command line will override
    # what may be specified in the config file for gae_download
    mongo_conn = pymongo.Connection(options.server, options.port)
    config = util.load_unstripped_json(options.config)
    insert_dumpfile_into_mongo(options.file, config, mongo_conn, 
                               options.offset, options.limit)
    

if __name__ == '__main__':
    main()    