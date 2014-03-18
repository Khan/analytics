#!/usr/bin/env python
"""
Dumps meta-data held within a knowledge model param file.

The file-format is defined in accuracy_model_train.py, in output_models().
"""

import argparse
import cStringIO
import csv
import pickle
import json


def parse_command_line():
    parser = argparse.ArgumentParser(description='Compare knowledge models.')

    parser.add_argument("knowledge_model_file",
        metavar="<path to pickled knowledge params>",
        help=("The path to the knowledge model params file, as output by "
            "accuracy_model_train.py"))

    parser.add_argument("-v", help="Enable verbose output.",
        action="store_const", const="True", dest="verbose")

    parser.add_argument("-t", help="Dump theta values as a CSV.",
        action="store_const", const="True", dest="dump_thetas")

    options = parser.parse_args()
    return options


def dump_metadata(options, knowledge_model):
    output_dict = {
        # Summarize the random components and thetas
        "components_count": len(knowledge_model["components"]),
        "components_size": len(knowledge_model["components"].values()[0]),
        "thetas_count": len(knowledge_model["thetas"]),
        "thetas_size": len(knowledge_model["thetas"].values()[0]),
    }

    if "training_info" in knowledge_model:
        training_info = knowledge_model["training_info"]

        # Clean things out that are overly verbose
        if "samples_per_exercise" in training_info and not options.verbose:
            del training_info["samples_per_exercise"]

        output_dict.update(training_info)

    print json.dumps(output_dict, indent=4)


def dump_thetas(options, knowledge_model):
    output = cStringIO.StringIO()
    csv_writer = csv.writer(output)

    # Add a header row
    thetas_size = len(knowledge_model["thetas"].values()[0])
    theta_labels = ["theta_%0.4d" % i for i in range(thetas_size)]
    csv_writer.writerow(["exercise"] + theta_labels)

    for exercise, thetas in knowledge_model["thetas"].iteritems():
        csv_writer.writerow([exercise] + thetas.tolist())

    print output.getvalue()


def main():
    options = parse_command_line()

    with open(options.knowledge_model_file, "r") as km_file:
        knowledge_model = pickle.load(km_file)

    if options.dump_thetas:
        dump_thetas(options, knowledge_model)
    else:
        dump_metadata(options, knowledge_model)


if __name__ == "__main__":
    main()
