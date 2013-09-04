#!/usr/bin/env python

"""Quick reducer script to match problemlog info to mindset messages from
website_request_logs where correspondences are given by pl.problem_number =
wrl.total_done.

Input:
    Tab-delimited rows of [bingo_id, info, dt, timestamp], where info can be
    either pl. or wrl.kalog (not clustered by ID)

Output:
    Emit bingo_id, problemlog info, mindset message text, and date
"""
from collections import defaultdict
import datetime as datet
from datetime import datetime
import sys


def main():
    kalogs = defaultdict(dict)
    plogs = defaultdict(dict)
    for line in sys.stdin:
            bingo_id, info, dt, timestamp = line.rstrip('\n').split('\t')

        # NOTE: the info variable for a kalog is a string like:
        # bingo.param:assessment_problem_attempt_binary
        # ;bingo.param:assessment_problem_attempt_count;bingo.param:
        # problem_attempt_binary;bingo.param:problem_attempt_count;bingo.param:
        # assessment_problem_correct_binary;bingo.param:
        # assessment_problem_correct_count;x.mindset.test_condition:control 
        # statement.positive statement;x.mindset.exercise_name:
        # adding_fractions_with_common_denominators;x.mindset.problem_type:
        # UNDEFINED;x.mindset.total_done:112;x.mindset.text:
        # %3Cp%3E%3Cem%3EAvoiding distractions like television makes learning 
        # much easier.%3C/em%3E%3C/p%3E;
        text_start = info.find('x.mindset.message_text') 
        total_done_start = info.find('x.mindset.total_done')
        kalog_exercise_name_start = info.find('x.mindset.exercise_name')

        # NOTE: the info variable for a problem log is a stringified dict like:
        # {
        #   "backup_timestamp": 1338072851.0, "earned_proficiency": true, 
        #   "attempts": ["\"276\""], "seed": "103", 
        #   "user": "0abedo1@woodburnsd.org", 
        #   "key": "<gigantic key> ", "topic_mode": false, "problem_type": "0",
        #   "problem_number": 10, "suggested": true, "count_hints": 0, 
        #   "sha1": "<gigantic hash>", "count_attempts": 1, 
        #   "ip_address": "67.22.242.55", "points_earned": 180, 
        #   "time_taken": 10, "review_mode": false, "time_done": 1338072850.0,
        #   "hint_used": false, "random_float": 0.59675686983286536, 
        #   "time_taken_attempts": [10], "correct": true, 
        #   "exercise": "subtraction_2"
        # }
        problem_number_start = info.find('"problem_number": ')
        plog_exercise_name_start = info.find('"exercise": ')

        # Ignore the -0700 offset, the timestamps for kalogs and problem logs
        # are both UTC actually.
        try:
            # kalog timestamps are of the form 23/08/2013:12:00:00 -0700 
            timestamp = timestamp.split()[0]
            timestamp = datetime.strptime(timestamp, "%d/%b/%Y:%H:%M:%S")
        except:
            # problemlog timestamps are a UTC timestamp like 1377358945.0
            timestamp = datetime.fromtimestamp(float(timestamp))

        if (text_start > -1 and total_done_start > -1 and
                kalog_exercise_name_start > -1):
            text_start += len('x.mindset.message_text') + 1
            text_end = info.find(';', text_start)
            text = info[text_start:text_end]

            total_done_start += len('x.mindset.total_done') + 1
            total_done_end = info.find(';', total_done_start)
            total_done = int(info[total_done_start:total_done_end])

            kalog_exercise_name_start += len('x.mindset.exercise_name') + 1
            kalog_exercise_name_end = info.find(';', kalog_exercise_name_start)
            kalog_exercise_name = info[kalog_exercise_name_start:
                                       kalog_exercise_name_end]

            kalogs[bingo_id][(kalog_exercise_name, total_done)] = {
                "text": text,
                "timestamp": timestamp,
                "dt": dt
            }

        elif (problem_number_start > -1 and plog_exercise_name_start > -1):
            problem_number_start += len('"problem_number": ')
            problem_number_end = info.find(',', problem_number_start)
            problem_number = int(info[problem_number_start:problem_number_end])

            plog_exercise_name_start += len('"exercise": ')
            plog_exercise_name_end = info.find('}', plog_exercise_name_start)
            plog_exercise_name = info[plog_exercise_name_start:
                                      plog_exercise_name_end]

            plogs[bingo_id][(plog_exercise_name, problem_number)] = {
                "info": info,
                "timestamp": timestamp,
                "dt": dt
            }

    for bingo_id, plog in plogs.iteritems():
        for (exercise, problem_number), plog_dict in plog.iteritems():
            exercise = exercise.strip('"')
            # for a given bingo_id, we match kalogs and plogs by the
            # correspondence total done <---> problem_number - 1
            if ((exercise, problem_number - 1) in kalogs[bingo_id].keys()) and
                    abs((kalogs[bingo_id][(
                    exercise, problem_number - 1)]["timestamp"] - 
                    plog_dict["timestamp"]).total_seconds()) < 600:
                print '\t'.join([bingo_id, 
                                 plog_dict["info"], 
                                 kalogs[bingo_id][(exercise, 
                                         problem_number - 1)]["text"],
                                 plog_dict["dt"]])
            else:
                print '\t'.join([bingo_id, plog_dict["info"],
                                 "", plog_dict["dt"]])


if __name__ == '__main__':
    main()
