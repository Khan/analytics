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

TODO(yunfang): Consider moving the primary functions here to a library
               as the pb_to_dict process doesn't really happen in hive
"""

import datetime
import json
import optparse
import pickle
import sys
import time

# This is the location of appengine in the map/reduce jar we create for EMR
sys.path.append("./google_appengine")
from google.appengine.api import datastore
from google.appengine.api import datastore_types
from google.appengine.api import users
from google.appengine.datastore import entity_pb


def get_cmd_line_args():
    parser = optparse.OptionParser(
            usage="%prog [options]",
            description=("Map script for converting "
                         "protobufs to (user, json) format"))
    parser.add_option("-k", "--key", default="key",
                     help="field corresponding to the reducer key")
    parser.add_option("-p", "--parent", default=None,
                     help="including parent key in the json dump")
    # TODO(yunfang): Output a warning with unknown args
    options, _ = parser.parse_args()
    return options


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
        if doc.year < 1970:
            return 0
        return time.mktime(doc.timetuple())
    elif isinstance(doc, basestring):
        if isinstance(doc, str):
            doc = unicode(doc, errors='replace')
        # Escape the newline character.
        return doc.replace("\n", "\\n")
    return doc


def pb_to_dict(pb, parent=None):
    """Convert a protocol buffer to a json-serializable dictionary"""
    entity = datastore.Entity._FromPb(entity_pb.EntityProto(pb))
    # create a json serializable dictionary from entity
    document = dict(entity)
    document['key'] = str(entity.key())
    if parent and entity.parent():
        document['parent'] = str(entity.parent())
    document = apply_transform(document)
    return document


def main():
    """Map step for the protobuf loading. Input is read from stdin."""
    options = get_cmd_line_args()
    entity_list = pickle.load(sys.stdin)
    for pb in entity_list:
        document = pb_to_dict(pb, options.parent)
        json_str = json.dumps(document)
        print "%s\t%s" % (document[options.key], json_str)


if __name__ == '__main__':
    main()
