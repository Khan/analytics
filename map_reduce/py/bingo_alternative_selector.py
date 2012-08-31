#!/usr/bin/env python

"""A reducer script that reads the identity of a user CROSS JOINed with
all the alternative values for an experiment and determines which alternative
bucket the user belongs to.

Input:
    Rows with tab delimited column values of
    [user, user_id, user_email, gae_bingo_identity, participating_tests,
        canonical_name, hashable_name, alternative_name, alternative_weight].

    Note that every single row will have the same canonical_name and
    hashable_name value, as this reducer is intended to analyze a single test.

Output:
    For each experiment a user is in, emits
    [user, user_id, user_email, canonical_name, alternative_name]
    for the alternative_name that the user belongs in. Users who are not
    participating in the test are ignored.
"""

import hashlib
import json
import sys


def main():
    current_user = None
    current_user_id = None
    current_user_email = None
    current_bingo_identity = None
    current_user_tests = None
    current_alternatives = None  # ordered tuples of (alt_name, weight)

    canonical_name = None
    hashable_name = None

    for line in sys.stdin:
        (user, user_id, user_email, bingo_identity, participating_tests,
                canonical_name, hashable_name, alternative_name, weight) = (
                        line.rstrip('\n').split('\t'))

        if user != current_user:
            if current_user:
                emit_alternative_for_user(
                        current_user,
                        current_user_id,
                        current_user_email,
                        current_bingo_identity,
                        canonical_name,
                        hashable_name,
                        current_alternatives)

            current_user = user
            current_user_id = user_id
            current_user_email = user_email
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

        current_alternatives.append((alternative_name, float(weight)))

    if current_user:
        emit_alternative_for_user(
                current_user,
                current_user_id,
                current_user_email,
                current_bingo_identity,
                canonical_name,
                hashable_name,
                current_alternatives)


def emit_alternative_for_user(
        user, user_id, user_email, bingo_identity,
        canonical_name, hashable_name, alternatives):
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
    for alternative, weight in sorted(alternatives,
                                      key=lambda (name, weight): weight,
                                      reverse=True):

        current_weight -= weight
        if index_weight >= current_weight:
            selected_alternative = alternative
            break

    if selected_alternative is None:
        # TODO(benkomalo): log error.
        return

    print "\t".join([
            user,
            user_id,
            user_email,
            canonical_name,
            selected_alternative
        ])


if __name__ == '__main__':
    main()
