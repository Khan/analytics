#!/usr/bin/env python
"""Utility script to upload a JSON MIRT model to GAE.

The filename of a JSON file containing the model is the only required option.

The only effect of the --create versus --update options is that --create mode
requires a 'title' and 'description' to be provided whereas --update does not.
If --update is used and no title or desription is provided, the server
will re-use the title and description for the previous model version
corresponding to the given slug.
"""

import json
import optparse

import oauth_util.fetch_url as oauth_fetcher


def require_property(property_name, model):
    assert property_name in model, (
            "Property %s is required for this mode." % property_name)


def upload_to_gae(filename, options):

    with open(filename) as f:
        model_json = f.read()

    model = json.loads(model_json)

    require_property("slug", model)
    require_property("engine_class", model)
    if options.create:
        require_property("title", model)
        require_property("description", model)

    print json.dumps(model, indent=4)
    # a bit redundant, but re-print the key properties so they are 
    # very visible when the user is prompted to confirm upload
    print "====="
    print "slug = '%s'" % model.get("slug")
    print "engine_class = '%s'" % model.get("engine_class")
    print "title = '%s'" % model.get("title")
    print "decription = '%s'" % model.get("description")
    print "====="

    ans = raw_input("Are you sure you want to upload to GAE? [y/N] ")
    if ans in ['Y', 'y']:
        oauth_fetcher.fetch_url(
            '/api/v1/dev/assessment/params', {'data': model_json})
    else:
        print "Canceling upload."


if __name__ == "__main__":
    parser = optparse.OptionParser()
    parser.add_option("-c", "--create", action="store_true", dest="create")
    parser.add_option("-u", "--update", action="store_false", dest="create")
    parser.set_defaults(create=True)

    options, args = parser.parse_args()

    if len(args) != 1:
        # TODO(jace): perhaps allow input from stdin (but the EOF makes
        # subsequent use of raw_input tough)
        exit("Filename of the JSON model is required as the sole "
             "command line argument.")

    upload_to_gae(args[0], options)
