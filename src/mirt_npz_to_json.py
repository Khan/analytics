#!/usr/bin/env python
"""Utility script to convert a .npz MIRT model to JSON.

Currently the MIRT training outputs data as a npz (NumPy) file.  This
script can convert it to JSON suitable for uploading to App Engine via
mirt_upload.py.

USAGE:

  python mirt_npz_to_json.py NPZFILE >out.json

The program will prompt for input on stderr, to avoid conflicting with
the written JSON on stdout.

TODO(jace): make the MIRT training output JSON and eliminate this script

"""

import json
import numpy
import sys


def generate_exercise_ind():
    # this is necessary because early MIRT models stored exercise indices
    # as a defaultdict that referenced a function by this name.
    # TODO(jace):  remove once the ouptut to npz is converted to
    # be a simple dict instead of a defaultdict.
    pass


def mirt_npz_to_json(npz_file):
    model = numpy.load(npz_file)

    num_exs = len(dict(model["exercise_ind_dict"][()]))

    couplings = model["couplings"]

    # only the first num_exs exercises correspond to an exercise;
    # the rest are junk/blank/.
    couplings = couplings[:num_exs, ]

    # TODO(jace): This is a HACK to flip sign of all the weights except the
    # one that matches the bias unit.  This should be removed once training
    # process is fixed always output the model with the natural sign.
    couplings[:, :-1] *= -1.0

    out_data = {
        "engine_class": "MIRTEngine",

        # MIRT specific data
        "params": {
            "exercise_ind_dict": dict(model["exercise_ind_dict"][()]),
            "couplings": couplings.tolist(),
            "max_length": 15
            }
        }

    slug = ""
    while not slug:
        print >>sys.stderr, "Enter the slug (required): ",
        slug = raw_input()
    
    print >>sys.stderr, ("Title can be left blank if you will be updating "
                         "an existing model.")
    print >>sys.stderr, "Enter the title (or hit enter for none): ",
    title = raw_input()
    
    print >>sys.stderr, ("Description can be left blank if you will be "
                         "updating an existing model.")
    print >>sys.stderr, "Enter the description (or hit enter for none): ",
    description = raw_input()

    if slug:
        out_data["slug"] = slug
    if title:
        out_data["title"] = title
    if description:
        out_data["description"] = description

    json_data = json.dumps(out_data, indent=4)

    print json_data


if __name__ == "__main__":
    if len(sys.argv) != 2:
        exit("Usage: %s input_filename" % sys.argv[0])

    filename = sys.argv[1]

    mirt_npz_to_json(filename)
