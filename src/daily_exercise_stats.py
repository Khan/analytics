#!/usr/bin/env python
import copy
import datetime
from optparse import OptionParser

import mongo_util
import util

userdata_db = None  
plog_db = None
report_db = None

g_logger = util.get_logger()

ex_collection_name = 'daily_ex_stats'
ex_mode_collection_name = 'daily_ex_mode_stats'

topic_modes = ['true', 'false']
topic_user_modes = ['none', 'some', 'majority', 'all'] 
user_modes = ['unknown', 'old', 'new', 'coached', 'uncoached',
              'heavy', 'light', 'registered', 'phantom']
everything_mode = ['everything']
all_modes = everything_mode + topic_modes + topic_user_modes + user_modes
super_modes = everything_mode + user_modes


def plog_shorten(plog):
    # done to conserve memory
    white_list = ['earned_proficiency', 'problem_number', 'time_done', 
                  'time_taken', 'hint_used', 'correct', 
                  'exercise', 'topic_mode']

    return dict([(f, plog[f]) for f in white_list if f in plog])


def load_for_day(day):

    range = {"$gte": day, "$lt": day + datetime.timedelta(days=1)}
    query = {"backup_timestamp": range}
    
    # load problem logs
    user_plogs = {}
    progress = util.LoopProgressLogger()
    for plog in plog_db['ProblemLog'].find(query):
        user = plog['user']
        if user not in user_plogs:
            user_plogs[user] = []
        user_plogs[user].append(plog_shorten(plog))
        progress.log(mod=10000, msg=str(plog['time_done']))
        
    # load user data corresponding to the day's ProblemLogs
    user_data_map = {}
    for user in user_plogs:
        user_data_map[user] = userdata_db['UserData'].find_one({'user': user})
        if len(user_data_map) % 1000 == 0:
            print "Loaded %d user_data docs." % len(user_data_map)

    return user_plogs, user_data_map


def compute_for_day(day, super_mode, filter_mode, user_plogs, user_data_map):

    print ("computing for day = %s, super_mode = %s, filter_mode = %s" % 
            (str(day), super_mode, filter_mode))
    
    stat_names = ['users', 'user_exercises', 'problems', 'correct', 'profs', 
                  'prof_prob_count', 'first_attempts', 'hint_probs', 
                  'time_taken']
    
    stats_init = dict([(s, 0) for s in stat_names])
    unique_users = {}
    
    ex_stats = {'ALL': copy.deepcopy(stats_init)}
        
    def user_day_matches_mode(plogs, user_data, mode):

        if mode in topic_user_modes:
            if not sum([1 for plog in plogs if 'topic_mode' in plog]):
                return False
            topic_modes = [p['topic_mode'] for p in plogs if 'topic_mode' in p]
            num_topic_plogs = sum(topic_modes)
            if mode == 'none':
                return num_topic_plogs == 0
            elif mode == 'some':
                return num_topic_plogs > 0
            elif mode == 'majority':
                return num_topic_plogs / float(len(plogs)) > .5
            elif mode == 'all':
                return num_topic_plogs == len(plogs)
        
        elif mode in user_modes:

            if user_data is None:
                return mode == 'unknown'

            if mode == 'unknown':
                return user_data is None  # always False
            elif mode == 'old':
                return ('joined' in user_data and
                        user_data['joined'] < 
                        day - datetime.timedelta(days=14))
            elif mode == 'new':
                return ('joined' in user_data and
                        user_data['joined'] >= 
                        day - datetime.timedelta(days=14))
            elif mode == 'coached':
                return ('coaches' in user_data and
                        len(user_data['coaches']) > 0)
            elif mode == 'uncoached':
                return ('coaches' not in user_data or
                        len(user_data['coaches']) <= 0)
            elif mode == 'heavy':
                return ('proficient_exercises' in user_data and 
                        len(user_data['proficient_exercises']) > 10)
            elif mode == 'light':
                return ('proficient_exercises' not in user_data or
                        len(user_data['proficient_exercises']) <= 10)
            elif mode == 'registered':
                return ('user_id' in user_data and
                        'nouserid' not in user_data['user_id'])
            elif mode == 'phantom':
                return ('user_id' in user_data and
                        'nouserid' in user_data['user_id'])

        return True
    
    def process_user_day(plogs, user, user_data):

        if not user_day_matches_mode(plogs, user_data, super_mode):
            return
        if not user_day_matches_mode(plogs, user_data, filter_mode):
            return
        
        # sort by time for any advanced inner-problem logic
        plogs = sorted(plogs, key=lambda p: p['time_done'])

        user_exs = []  # keep a list of all exs done by this user
        
        for plog in plogs:
            ex = plog['exercise']

            if filter_mode in ['true', 'false']:
                if plog.get('topic_mode') != (filter_mode == 'true'):
                    continue
            
            unique_users[user] = True
            if ex not in ex_stats:
                ex_stats[ex] = copy.deepcopy(stats_init)  # blank stats

            stats = ex_stats[ex]
            if ex not in user_exs:
                stats['users'] += 1
                user_exs.append(ex)
            stats['user_exercises'] = stats['users']
            stats['problems'] += 1
            stats['correct'] += plog['correct']
            stats['profs'] += plog['earned_proficiency']  
            if plog['earned_proficiency']:
                stats['prof_prob_count'] += int(plog['problem_number'])  
            stats['first_attempts'] += (plog['problem_number'] == 1)
            stats['hint_probs'] += plog['hint_used']
            stats['time_taken'] += max(0, min(600, int(plog['time_taken'])))

    for user in user_plogs:
        process_user_day(user_plogs[user], user, user_data_map[user])
        
    # merge all individual exercise stats into the global/aggregated stat set
    for ex in ex_stats:
        if ex != 'ALL':
            for stat in ex_stats[ex]:
                ex_stats['ALL'][stat] += ex_stats[ex][stat]
    # but we need to correct the user count-- de-dupe it
    ex_stats['ALL']['users'] = len(unique_users)
    
    # first remove any pre-existing data for _this_ date
    query = {'date': day, 'super_mode': super_mode, 'filter_mode': filter_mode}
    report_db[ex_collection_name].remove(query)  
    # now update the reporting db
    for ex in ex_stats:
        s = ex_stats[ex]
        if s['problems'] <= 0: 
            continue
        s['date'] = day  # for queries
        s['exercise'] = ex  # for queries
        s['super_mode'] = super_mode
        s['filter_mode'] = filter_mode
        s['avg_probs_til_prof'] = s['prof_prob_count'] / (s['profs'] + 1.0)

        report_db[ex_collection_name].insert(s)
        
    # finally, analyze the composition of unique users for this mode
    cross_sectional_modes = topic_user_modes + user_modes
    composition = dict([(mode, 0) for mode in cross_sectional_modes])
    for user in unique_users:
        for mode in cross_sectional_modes:
            composition[mode] += user_day_matches_mode(user_plogs[user], 
                                                       user_data_map[user], 
                                                       mode) 
    composition.update({'super_mode': super_mode, 
                        'filter_mode': filter_mode, 
                        'date': day, 
                        'count': len(unique_users)})

    # and stick in the reporting db, too
    query = {'date': day, 'super_mode': super_mode, 'filter_mode': filter_mode}
    report_db[ex_mode_collection_name].remove(query)  
    report_db[ex_mode_collection_name].insert(composition)
    

def run_for_day(day):
    user_plogs, user_data_map = load_for_day(day)

    # This report allows 2 levels of cross-sectioning/subsetting.  That is,
    # you can view the cross-section of a cross-section.  The highest level
    # cross-section is called the 'super_mode', and the cross-section taken
    # within the super_mode is called simple the 'mode'.  
    
    for super_mode in super_modes:
        for mode in all_modes:
            compute_for_day(day, super_mode, mode, user_plogs, user_data_map)
                

def compute_daily_exercise_statistics(start_day, end_day):
    day = end_day  
    while day >= start_day:
        run_for_day(day)
        day -= datetime.timedelta(days=1)
        

def init_databases(config_file):
    global userdata_db, plog_db, report_db
    userdata_db = mongo_util.get_db('entities_main', config_file)  
    plog_db = mongo_util.get_connection('datastore', config_file)['kadb_pl']
    report_db = mongo_util.get_db('reporting', config_file)
    
    
def main():
    desc = "Generates daily time series of statistics on exercise usage."
    parser = OptionParser(usage="%prog [options]", description=desc)
    parser.add_option("-b", "--begindate", help="In format YYYY-MM-DD.")
    parser.add_option("-e", "--enddate", help="In format YYYY-MM-DD.")
    parser.add_option("-c", "--config", 
                      help="Full path to analytics.json or equivalent.")
    (options, dummy) = parser.parse_args()

    if not options.config:
        g_logger.fatal("Please specify JSON config file to use (via -c).")
        exit(1)

    init_databases(options.config)
    
    if options.begindate and options.enddate:
        start_date = datetime.datetime.strptime(options.begindate, "%Y-%m-%d")
        end_date = datetime.datetime.strptime(options.enddate, "%Y-%m-%d")
    else:
        #pymongo uses datetime only (not date)
        today = datetime.datetime.combine(datetime.date.today(), 
                                          datetime.time())
        yesterday = today - datetime.timedelta(days=1) 
        start_date = end_date = yesterday
        
    compute_daily_exercise_statistics(start_date, end_date)
    

if __name__ == '__main__':
    main()
