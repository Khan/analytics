#!/usr/bin/env python

"""A reducer script that reads the identity of a user CROSS JOINed with
all the alternative values for an experiment and determines which alternative
bucket the user belongs to.

Input:
    Rows with tab delimited column values of
    [bingo_identity, participating_tests,
        canonical_name, hashable_name, alternative_name, alternative_weight].

    Note that every single row will have the same canonical_name and
    hashable_name value, as this reducer is intended to analyze a single test.

Output:
    For each experiment a user is in, emits
    [bingo_identity, canonical_name, alternative_name]
    for the alternative_name that the user belongs in. Users who are not
    participating in the test are ignored.
"""

import hashlib
import json
import sys


def main():

    current_bingo_identity = None
    current_user_tests = None
    current_alternatives = None  # ordered tuples of (alt_name, weight)

    canonical_name = None
    hashable_name = None

    for line in sys.stdin:
        (bingo_identity, participating_tests,
                canonical_name, hashable_name, alternative_name, weight,
                alternative_number) = (
                        line.rstrip('\n').split('\t'))

        if bingo_identity != current_bingo_identity:
            if current_bingo_identity:
                emit_alternative_for_user(
                        current_bingo_identity,
                        canonical_name,
                        hashable_name,
                        current_alternatives)

            current_bingo_identity = bingo_identity

            # participating_tests is a list of test names in the form of
            # "some canonical name (conversion name)". We only care about the
            # canonical name here.
            parsed_tests = json.loads(participating_tests)
            current_user_tests = set([t.split('(')[0].strip()
                                      for t in parsed_tests])
            current_alternatives = []

        if canonical_name not in current_user_tests:
            continue

        current_alternatives.append((alternative_name, long(weight), 
                                      alternative_number))

    if current_bingo_identity:
        emit_alternative_for_user(
                current_bingo_identity,
                canonical_name,
                hashable_name,
                current_alternatives)


def emit_alternative_for_user(
            bingo_identity, canonical_name, hashable_name, alternatives):
    """Determines the experiment alternative for the given user and prints it.

    This *must* be consistent with modulo_choose in gae_bingo.py.
    """

    if not alternatives:
        # No alternative info - user must not be participating in the test.
        return

    alternatives_weight = sum([weight for (name, weight) in alternatives])

    sig = hashlib.md5(hashable_name + str(bingo_identity)).hexdigest()
    sig_num = int(sig, base=16)
    index_weight = sig_num % alternatives_weight

    current_weight = alternatives_weight
    selected_alternative = None

    # Sorting by weight and then number keeps the sort stable. 
    # We sort by name last, which keeps the sort stable for records
    # created before the number was created.
    for alternative, weight in sorted(alternatives,
                                      key=lambda (name, weight, number):
                                             (weight, number, name)
                                      reverse=True):

        current_weight -= weight
        if index_weight >= current_weight:
            selected_alternative = alternative
            break

    if selected_alternative is None:
        # TODO(benkomalo): log error.
        return

    print "\t".join([
            bingo_identity,
            canonical_name,
            selected_alternative    
            ])

    
if __name__ == '__main__':
    main()
