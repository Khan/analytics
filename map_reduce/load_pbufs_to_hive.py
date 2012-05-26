#!/usr/bin/python
""" Mapper script for processing protocol buffers to json strings on EMR hive.
The point of this script is to convert the protocol buffers downloaded
from GAE to json encoded datastore hosted on Amazon S3. The data on S3 can then
 be queried using the elastic mapreduce framework.
Input is a collection of protocol buffers taken from stdin. Each protocol
buffer is an entity from GAE such as an entity from UserData, ProblemLog,
VideoLog and etc.
Output is a series of lines piped to stdout:
username<tab>json_encoded_entities
Check out https://sites.google.com/a/khanacademy.org/forge/technical/data_n/running-emr-elastic-mapreduce-on-the-khan-academy-data
for more information about running EMR at the Khan Academy
"""

import datetime
import json
import os
import pickle
import sys
import time

# This is the location of appengine in the map/reduce jar we create for EMR
sys.path.append("./google_appengine")
from google.appengine.api import datastore
from google.appengine.api import datastore_types
from google.appengine.api import users
from google.appengine.datastore import entity_pb


def apply_transform(doc):
    """Apply transformation to GAE entities to be json serializable."""
    if isinstance(doc, dict):
        for key, value in doc.iteritems():
            doc[key] = apply_transform(value)
        return doc
    elif isinstance(doc, list):
        doc = [apply_transform(item) for item in doc]
        return doc
    elif (isinstance(doc, datastore_types.Key) or
          isinstance(doc, users.User)):
        return str(doc)
    elif isinstance(doc, datetime.datetime):
        return time.mktime(doc.timetuple())
    return doc


def main():
    """Map step for the protobuf loading. Input is read from stdin."""
    if len(sys.argv) > 1 and (sys.argv[1] == '-h' or sys.argv[1] == '--help'):
        print "Map script for converting protobufs to (user, json) format\n"
        print "\t This script takes no arguments and processes data from stdin."
        print " Output can be directly queryable with hive.\n"
        exit(1)

    entity_list = pickle.load(sys.stdin)
    for pb in entity_list:
        entity = datastore.Entity._FromPb(entity_pb.EntityProto(pb))
        # create a json serializable dictionary from entity
        document = dict(entity)
        document['key'] = str(entity.key())
        document = apply_transform(document)
        json_str = json.dumps(document)
        print "%s\t%s\n" % (document['user'], json_str)


if __name__ == '__main__':
    main()
