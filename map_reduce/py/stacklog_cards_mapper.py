#!/usr/bin/env python

"""A hive streaming mapper script that takes tab-delimited stack logs (rows
from the stacklog table) from stdin and outputs a line for each exercise card
done in topic mode.
"""

# TODO(david): Write this in Hive SQL?
# TODO(david): Tests


import json
import sys


def main():

    for line in sys.stdin:

        user_id, data_json, date = line.strip().split('\t')
        data = json.loads(data_json)

        # TODO(david): Get stacks from practice mode as well.
        if not data['topic_mode']:
            continue

        topic_id = data['topic_id']
        cards_list = json.loads(data['cards_list'])

        for card in cards_list:

            if 'associated_log' not in card:
                continue

            associated_log = card['associated_log']
            if 'ProblemLog' not in associated_log:
                continue

            if 'card' not in card:
                continue

            scheduler_info = json.dumps(card['card'].get('schedulerInfo', {}))

            # TODO(david): This is where we stick extra information about the
            #     user, such as A/B test experiments they are being subjected
            #     to, card selection algorithm, etc. Possibly comma-delimited
            #     for multiple experiments/segments, or a JSON array, or a Hive
            #     array?
            user_segment = "control"

            print '\t'.join([associated_log['ProblemLog'], user_id, topic_id,
                scheduler_info, user_segment])


if __name__ == '__main__':
    main()
