#!/usr/bin/env python

"""Hive reducer script to compute daily exercise stats."""

import collections
import datetime
import json
import sys

# global string var representing the date partition we're working with
g_dt = None

"""Declare the filter modes which can be used to cross-section the data.

This report allows 2 levels of cross-sectioning/subsetting.  That is,
you can view the cross-section of a cross-section.  The highest level
cross-section is called the 'super_mode', and the cross-section taken
within the super_mode is called the 'sub_mode'.  For example, for
super_mode = 'coached' and sub_mode = 'majority', statistics are
compute only over coached users that did a majority of their problems
for that day in topic_mode.
"""
# NOTE: while other filter modes are determined on an all-or-nothing basis
# for the set of a user's plogs on a given day, topic_modes are different--
# each plog is individually kept or tossed depending on whether it matches
# the specified value of topic_mode
topic_modes = ['true', 'false']
topic_user_modes = ['none', 'some', 'majority', 'all']
user_modes = ['unknown', 'old', 'new', 'coached', 'uncoached',
              'heavy', 'light', 'registered', 'phantom']
everything_mode = ['everything']

# now we define the lists of modes we will iterate over
super_modes = everything_mode + user_modes
sub_modes = everything_mode + topic_modes + topic_user_modes + user_modes


def compute(sub_mode, plogs):
    # dict of dicts, keyed by exercise name, then stat name
    ex_stats = collections.defaultdict(lambda: collections.defaultdict(int))

    for plog in plogs:
        ex = plog['exercise']

        if sub_mode in topic_modes:
            # this type of filter is decided on a per-problem basis
            if plog.get('topic_mode') != (sub_mode == 'true'):
                continue

        if ex not in ex_stats:
            ex_stats[ex]['users'] = 1

        stats = ex_stats[ex]
        stats['user_exercises'] = stats['users']
        stats['problems'] += 1
        stats['correct'] += plog['correct']
        stats['profs'] += plog['earned_proficiency']
        if plog['earned_proficiency']:
            stats['prof_prob_count'] += int(plog['problem_number'])
        stats['first_attempts'] += (plog['problem_number'] == 1)
        stats['hint_probs'] += plog['hint_used']
        stats['time_taken'] += max(0, min(600, int(plog['time_taken'])))

    # merge all individual exercise stats into the global/aggregated stat set
    if ex_stats:
        for ex in ex_stats.keys():
            for stat in ex_stats[ex]:
                ex_stats['ALL'][stat] += ex_stats[ex][stat]
        # but we need to correct the user count-- de-dupe it
        ex_stats['ALL']['users'] = 1

    return ex_stats


def output(super_mode, sub_mode, ex_stats):
    for ex, stats in ex_stats.iteritems():
        stat_names = ['users', 'user_exercises', 'problems', 'correct',
                'profs', 'prof_prob_count', 'first_attempts', 'hint_probs',
                'time_taken']
        output_str = "\t".join([super_mode, sub_mode, ex]) + "\t"
        output_str += "\t".join([str(stats[s]) for s in stat_names]) + "\n"
        sys.stdout.write(output_str)


def num_topic_plogs(plogs):
    # if the topic_mode property doesn't even exist, don't compute
    if not sum([1 for plog in plogs if 'topic_mode' in plog]):
        return None

    topic_modes = [p['topic_mode'] for p in plogs if 'topic_mode' in p]
    return sum(topic_modes)


def user_day_matches_mode(plog_stats, user_info, mode):
    num_plogs, num_topic_plogs = plog_stats

    if mode in topic_user_modes:
        if num_topic_plogs is None:  # the topic_property didn't even exist
            return False

        if mode == 'none':
            return num_topic_plogs == 0
        elif mode == 'some':
            return num_topic_plogs > 0
        elif mode == 'majority':
            return num_topic_plogs / float(num_plogs) > .5
        elif mode == 'all':
            return num_topic_plogs == num_plogs

    elif mode in user_modes:

        as_of_date = datetime.datetime.strptime(g_dt, '%Y-%m-%d')

        if user_info is None:
            # if we don't have user_info, don't pretend we can decide on
            # modes other than 'unknown'
            return mode == 'unknown'

        # we've delayed as long as possible.. it's time to parse the json
        user_info = json.loads(user_info[1])

        if mode == 'unknown':
            return user_info is None  # always False, due to preceeding lines
        elif mode == 'old':
            return ('joined' in user_info and
                    datetime.datetime.fromtimestamp(user_info['joined']) <
                    as_of_date - datetime.timedelta(days=14))
        elif mode == 'new':
            return ('joined' in user_info and
                    datetime.datetime.fromtimestamp(user_info['joined']) >=
                    as_of_date - datetime.timedelta(days=14))
        elif mode == 'coached':
            return ('coaches' in user_info and
                    len(user_info['coaches']) > 0)
        elif mode == 'uncoached':
            return ('coaches' not in user_info or
                    len(user_info['coaches']) <= 0)
        elif mode == 'heavy':
            return ('proficient_exercises' in user_info 
                    and
                    # TODO(jace) - WTF is this necessary?  sometimes, the
                    # user_info['proficient_exercises'] is list, and I don't
                    # now why... hack for now and bail if type is not string.
                    (isinstance(user_info['proficient_exercises'], basestring) 
                    and
                    len(user_info['proficient_exercises'].split("\t")) > 10))
        elif mode == 'light':
            return ('proficient_exercises' not in user_info 
                    or
                    (isinstance(user_info['proficient_exercises'], basestring) 
                    and 
                    len(user_info['proficient_exercises'].split("\t")) <= 10))
        elif mode == 'registered':
            return ('user_id' in user_info and
                    'nouserid' not in user_info['user_id'])
        elif mode == 'phantom':
            return ('user_id' in user_info and
                    'nouserid' in user_info['user_id'])

    # ELSE mode is either topic_mode--which is decided on a per-problem basis,
    # or it was 'everything', which should always match
    return True


def process_user_day(user_info, plogs):
    """Compute statistics over plogs for each comination of filter modes."""

    plog_stats = (len(plogs), num_topic_plogs(plogs))

    # The stats will only differ based on whether the sub_mode is
    # 'true', 'false', or some other value (in which case all plogs) are
    # counted.  So for performance, pre-compute those three summaries.
    ex_stats = {}
    ex_stats['true'] = compute(sub_mode='true', plogs=plogs)
    ex_stats['false'] = compute(sub_mode='false', plogs=plogs)
    ex_stats[None] = compute(sub_mode='everything', plogs=plogs)

    # Now, as we iterate through pair of filter modes, we only have to pick
    # 1 of the 3 pre-computed options to output.
    for super_mode in super_modes:
        for sub_mode in sub_modes:
            if (user_day_matches_mode(plog_stats, user_info, super_mode) and
                    user_day_matches_mode(plog_stats, user_info, sub_mode)):
                # Choose the right pre-computed stats for this mode pair
                if sub_mode in topic_modes:
                    output(super_mode, sub_mode, ex_stats[sub_mode])
                else:
                    output(super_mode, sub_mode, ex_stats[None])


def main():
    if len(sys.argv) <= 1:
        print >> sys.stderr, "Usage: %s <dt>" % sys.argv[0]
        exit(1)
    global g_dt
    g_dt = sys.argv[1]

    user_info = None
    prev_user = None
    plogs = []
    value_errors = 0
    join_errors = 0

    for line in sys.stdin:
        user, type_str, json_str = line.rstrip('\n').split('\t')

        # If we're about to switch users, process the buffer and empty it
        if user != prev_user:
            if user_info is not None and user_info[0] != prev_user:
                join_errors += 1
            else:
                process_user_day(user_info, plogs)
            plogs = []

        # If the line type represents user_info, all we need to do is keep
        # the lastest value we've seen.
        if type_str == "user_info":
            user_info = (user, json_str)  # delay JSON parsing for perf
            continue

        # Otherwise, the line type is a ProblemLog.  Buffer it until we
        # have all of the ProblemLogs for this user
        prev_user = user
        try:
            plog = json.loads(json_str)
            plogs.append(plog)
        except ValueError:
            value_errors += 1

    process_user_day(user_info, plogs)

    print >>sys.stderr, "Finished main with %d ValueErrors " % value_errors
    print >>sys.stderr, "and %d join errors." % join_errors

if __name__ == '__main__':
    main()
