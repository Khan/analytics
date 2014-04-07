#!/usr/bin/env python
"""
Dumps meta-data held within a knowledge model param file.
Also, provides a "compact" command that can be used to compact a
knowledge_model with the minimum data needed for production.

The file-format is defined in accuracy_model_train.py, in output_models().
"""

import argparse
import cStringIO
import csv
import os
import pickle
import json


def parse_command_line():
    parser = argparse.ArgumentParser(description='Compare knowledge models.')

    parser.add_argument("knowledge_model_file",
        metavar="<path to pickled knowledge params>",
        help=("The path to the knowledge model params file, as output by "
            "accuracy_model_train.py"))

    parser.add_argument("-v", action="store_const", const="True",
        dest="verbose",
        help="Enable verbose output.")

    parser.add_argument("-t", "--thetas", action="store_const", const="True",
        dest="dump_thetas",
        help="Dump theta values as a CSV.")

    parser.add_argument("-f", "--thresholds", action="store_const",
        const="True", dest="dump_thresholds",
        help="Dump threshold values as a CSV.")

    parser.add_argument("-c", "--compact", action="store_const",
        const="True", dest="compact",
        help="Remove unnecessary data from a knowledge model file.")

    options = parser.parse_args()
    return options


def dump_metadata(options, knowledge_model):
    """Dumps top-level knowledge model metadata in JSON dict format."""
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

    if "f_thresholds" in knowledge_model:
        example_threshold = knowledge_model["f_thresholds"].values()[0]
        output_dict.update({
            "f_subscripts": example_threshold.keys(),
            "f_thresholds_count": len(knowledge_model["f_thresholds"]),
        })

    print json.dumps(output_dict, indent=4)


def dump_thetas(options, knowledge_model):
    """Dumps theta values to CSV format."""
    output = cStringIO.StringIO()
    csv_writer = csv.writer(output)

    # Add a header row like exercise,theta_0000,theta_0001,...,theta_N
    thetas_size = len(knowledge_model["thetas"].values()[0])
    theta_labels = ["theta_%0.4d" % i for i in range(thetas_size)]
    csv_writer.writerow(["exercise"] + theta_labels)

    for exercise, thetas in knowledge_model["thetas"].iteritems():
        csv_writer.writerow([exercise] + thetas.tolist())

    print output.getvalue()


def dump_thresholds(options, knowledge_model):
    """Dumps exercise-specific recommendation thresholds to CSV format."""
    output = cStringIO.StringIO()
    csv_writer = csv.writer(output)
    csv_writer.writerow(["exercise", "subscript", "max_score", "success",
        "threshold", "samples"])

    for exercise, scores in knowledge_model["f_thresholds"].iteritems():
        for subscript, result in scores.iteritems():
            csv_writer.writerow([exercise, subscript,
                result["max_score"], result["success"], result["threshold"],
                result["samples"]])

    print output.getvalue()


def compact(options, knowledge_model):
    """Deletes unnecessary information from the knowledge_model."""
    for exercise, scores in knowledge_model["f_thresholds"].iteritems():
        # TODO(mattfaus): Delete subscript entries that we are not using
        for subscript, result in scores.iteritems():
            # Only the threshold is needed in production
            knowledge_model["f_thresholds"][exercise][subscript] = {
                "threshold": result["threshold"],
            }

    size_before = os.stat(options.knowledge_model_file).st_size
    print "%s size before compaction %d" % (options.knowledge_model_file,
        size_before)

    with open(options.knowledge_model_file, "w") as km_file:
        pickle.dump(knowledge_model, km_file)

    size_after = os.stat(options.knowledge_model_file).st_size
    print "%s size after compaction  %d" % (options.knowledge_model_file,
        size_after)
    print "%d bytes saved" % (size_before - size_after)
    # Hm, looks like this only saves ~1.5%, so maybe not worth it.


def main():
    options = parse_command_line()

    with open(options.knowledge_model_file, "r") as km_file:
        knowledge_model = pickle.load(km_file)

    if options.dump_thetas:
        dump_thetas(options, knowledge_model)
    elif options.dump_thresholds:
        dump_thresholds(options, knowledge_model)
    elif options.compact:
        compact(options, knowledge_model)
    else:
        dump_metadata(options, knowledge_model)


if __name__ == "__main__":
    main()
