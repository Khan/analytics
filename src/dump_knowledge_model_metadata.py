#!/usr/bin/env python
"""
Dumps meta-data held within a knowledge model param file.

The file-format is defined in accuracy_model_train.py, in output_models().
"""

import argparse
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

    options = parser.parse_args()
    return options


def main():
    options = parse_command_line()

    with open(options.knowledge_model_file, "r") as km_file:
        knowledge_model = pickle.load(km_file)

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


if __name__ == "__main__":
    main()
