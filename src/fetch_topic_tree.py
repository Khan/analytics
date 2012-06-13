#!/usr/bin/env python

"""Script to fetch topic tree information from the main GAE server.

This does not fetch information about topic tree version, as that information
is captured in a separate download process that backs up raw entities. Instead,
it reads that information about topic tree version, and fetches the actual
content of the topic tree.

All data is stored in Mongo.
"""

import json
import pymongo
import urllib2

import util


logger = util.get_logger()


def unfetched_trees(mongodb):
    """Generator to iterate over the topic versions not yet downloaded."""
    versions = mongodb['TopicVersion'].find()

    for topic_version in versions:
        version_id = topic_version['number']
        tree = mongodb['TopicTree'].find({'_id': version_id})
        if tree.count() == 0:
            yield version_id


def fetch_topic_tree(mongodb, version_id):
    """Given a version_id, fetch the full topic tree info and save to mongo."""

    base_url = "http://www.khanacademy.org"
    url = "%s/api/v1/topicversion/%s/topictree" % (base_url, version_id)

    logger.info("Fetching tree for version [%s]" % version_id)
    try:
        response = urllib2.urlopen(url)
    except urllib2.HTTPError, e:
        # TODO(benkomalo): handle and retry
        logger.error("Unable to fetch [%s]" % version_id)
        logger.error(e)
        return

    tree = json.loads(response.read())
    # Use the version ID as the mongo primary key
    tree['_id'] = version_id

    logger.info("Fetch successful. Saving [%s]" % version_id)
    mongodb['TopicTree'].save(tree)


def main():
    mongodb = pymongo.Connection(port=12345)['kadb']

    for version_id in unfetched_trees(mongodb):
        fetch_topic_tree(mongodb, version_id)


if __name__ == '__main__':
    main()

