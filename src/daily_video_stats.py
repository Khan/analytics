#!/usr/bin/env python
"""Scripts to process VideoLog to produce a daily summary of 
    * number of users watched a video
    * number of users completed a video
    * total seconds watched for a video
  as well as totals. The output will be written to the report db
"""
import copy
import datetime
from optparse import OptionParser
import re

import pymongo

import gae_util
gae_util.fix_sys_path()

import util
import date_util

vid2title = {}
g_logger = util.get_logger()

def get_cmd_line_args():
    parser = OptionParser(usage = "%prog [options]", 
        description="processing video logs for summary stats")
    parser.add_option("-d", "--day", help="YYYY-mm-dd")
    options, _ = parser.parse_args()
    
    if not options.day:
        g_logger.fatal("Please specify the day")
        exit(1)
    return options

def get_data(day_str): 
    """Get data from mongo"""
    # TODO(yunfang): parameterize this thing
    vlog_collection = pymongo.Connection(port=12345)['kadb_vl']['VideoLog']
    iso_str = "%sT00:00:00Z" % day_str
    day = date_util.from_date_iso(iso_str)
    query = {"backup_timestamp": 
                {"$gte": day,
                 "$lt": day + datetime.timedelta(days=1)}
            }
    sort_spec = [('user', 1)]
    g_logger.info("Processing VideoLog for %s" % day_str)
    return vlog_collection.find(query, sort = sort_spec)

def update(data, category_list, key, val, incr = True): 
    """Update the basic bi-level dictionary of data[vid][property]"""
    for cat in category_list:
        if cat not in data:
            data[cat] = {}
        if key not in data[cat] or not incr:
            data[cat][key] = val
        else:
            data[cat][key] += val

def update_dict(to_dict, from_dict): 
    """Add the from_dict data to to_dict"""
    for cat, kv_pair in from_dict.iteritems():
        if cat not in to_dict:
            to_dict[cat] = {}
        for key, val in kv_pair.iteritems():
            if key not in to_dict[cat]:
                to_dict[cat][key] = val 
            else:
                to_dict[cat][key] += val

def update_summary(global_summary, user_summary, user_categories): 
    """Update the global summary with user_summary and user_categories """
    for ucat in user_categories:
        if ucat not in global_summary:
            global_summary[ucat] = {}
        cat_summary = global_summary[ucat]
        update_dict(cat_summary, user_summary)         
    
def analyze_log_for_user(video_log):
    """Get the video watching summary for a user """
    user_summary = {}
    global vid2title
    if len(video_log) == 0:
        return
    for rec in video_log: 
        secs_watched = rec["seconds_watched"]
        complete = bool(rec["is_video_completed"]) 
        vid_key = rec['youtube_id']
        vid2title[vid_key] = rec['video_title']
        update(user_summary, [vid_key, 'total'], 
               "seconds_watched", secs_watched) 
        update(user_summary, [vid_key], "watched", 1, False) 
        if complete:
            update(user_summary, [vid_key], "completed", 1, False) 
    # summarize on the # videos completed
    num_completed = 0;
    num_watched = 0;
    for key, kv_pair in user_summary.iteritems():
        if key == 'total':
            continue
        num_watched += 1
        if 'completed' in kv_pair:
            num_completed += 1
    user_summary['total']['completed'] = num_completed                   
    user_summary['total']['watched'] = num_watched                   
    # Track # users watching videos
    user_summary['total']['user'] = 1
    return user_summary

def get_user_categories(user):
    """Get user categories. Currently very crude"""
    # TODO(yunfang): use jace's classification
    categories = ['all']
    matched = re.match(r'.*nouserid.khanacademy.org', user)
    #user logged in or not
    if matched is None:
        categories.append('registered')
    else:
        categories.append('phantom')
    return categories
    
def analyze_all(data): 
    """Analyze all the data from the query output """
    global_summary = {}
    user = None
    rec_list = []
    num_users = 0 
    num_recs = 0
    for rec in data:
        num_recs += 1
        if rec['user'] == user:
            rec_list.append(rec)
        else:
            if len(rec_list) > 0: 
                user_summary = analyze_log_for_user(rec_list) 
                user_categories = get_user_categories(user)
                update_summary(global_summary, user_summary, user_categories)  
                num_users += 1
                if num_users % 1000 == 0:
                    g_logger.info("%s users processed with %s records" % 
                                  (num_users, num_recs))
            rec_list = [rec]
            user = rec['user']
    # bounday condition      
    if len(rec_list) > 0: 
	user_summary = analyze_log_for_user(rec_list) 
	user_categories = get_user_categories(user)
	update_summary(global_summary, user_summary, user_categories)  
    return global_summary 

def populate(summary, day_str):
    """Populate the data to the report db"""
    # TODO(yunfang): parameterize this thing
    report_db = pymongo.Connection('10.212.150.79')['report']
    #report_db = pymongo.Connection(port=12345)['report']
    report_collection = report_db['daily_video_stats']
    iso_str = "%sT00:00:00Z" % day_str
    day = date_util.from_date_iso(iso_str)
    for ucat, vid2stats in summary.iteritems():
        for vid, stats in vid2stats.iteritems():  
            if vid in vid2title:
                title = vid2title[vid]
            else:
                title = 'total'    
            doc = {"day": day, "date_str": day_str, 
                   "ucat": ucat, "vid": vid, 'vtitle':title}
            doc.update(stats)
            doc["_id"] = "%s-%s-%s" % (ucat, vid, day_str)
            report_collection.save(doc)
def main():
    options = get_cmd_line_args()
    global_summary = analyze_all(get_data(options.day)) 
    populate(global_summary, options.day)  
if __name__ == '__main__':
    main()

    

