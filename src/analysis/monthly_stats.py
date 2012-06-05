#!/usr/bin/env python
"""Compute monthly time series related to engagement and retention.  

This file was created in preparation for the KA Board meeting in June 2012.

The monthly_* functions all populate a mongo collection of documents with 
keys: {_id, tag, month, value}.  The output_collection argument specifies
which collection data is inserted into.  Each document represents a data 
point in a monthly time series identified by the tag property.

The retention_cohort_analysis also generates monthly time series, but is 
specific to a "cohort" of users, which in this context is a group of 
email-registered users that joined in the same calendar month.  The cohorts 
are named according to that month.
"""

import re

import bson
import pymongo

import mongo_examples
import mongo_util
import util

db_name = 'kadb'  # TODO(jace): make configurable
mongo = pymongo.Connection() 
mongo_db = mongo[db_name]

g_logger = util.get_logger()


def code(js):
    """Convert Javascript string to binary format that PyMongo requires."""
    return bson.Code(js)


def monthly_video_exercise_activity(output_collection):
    """Count users with minimum distinct video and exercise views.
    
    Creates a time series of data points (ie, mongo documents) formatted as
    described in the file docstring and inserts them in the ouput_collection
    for each activity threshold tuple in a list (which is currently hardcoded 
    below). The thresholds for each series are specified as a 
    (min_vid, min_ex) tuple.  min_vid is the minimum number of distinct videos
    at least partially viewed by the user within each month.  min_ex is the 
    minimum number of distinct exercises with at least one problem done by the 
    user within each month.  For a user to be counted, he must meet or exceed
    BOTH thresholds.
    """
    
    #TODO(jace): make configurable
    thresholds = [(0, 0), (1, 0), (5, 0), (10, 0), (25, 0), (0, 1), 
                 (0, 5), (0, 10), (0, 25), (1, 1), (5, 5)]
    
    map_vids_js = """
    function () {
        var month = this.last_watched.getMonth()+1;
        var padded_month = month < 10 ? "0"+month : month;
        var date_key = (this.last_watched.getYear()+1900) + "-" + padded_month;
        var key = date_key + "|" + this.user;
        emit( key, {videos: 1, exercises: 0} );
    } 
    """
    map_exs_js = """
    function () {
        var month = this.first_done.getMonth()+1;
        var padded_month = month < 10 ? "0"+month : month;
        var date_key = (this.first_done.getYear()+1900) + "-" + padded_month;
        var key = date_key + "|" + this.user;
        emit( key, {videos: 0, exercises: 1} );
    }
    """
    reduce_js = """
    function(key, values) {
        var result = {videos: 0, exercises: 0};
    
        values.forEach(function(value) {
          result.videos += value.videos;
          result.exercises += value.exercises;
        });
        
        return result;
    }
    """
    
    mongo_db['UserVideo'].map_reduce(
                             code(map_vids_js), 
                             code(reduce_js), 
                             out="vid_ex_users")
    g_logger.info("UserVideo map complete")

    mongo_db['UserExercise'].map_reduce(code(map_exs_js), 
                                code(reduce_js), 
                                out={'reduce': 'vid_ex_users'}) 
    g_logger.info("UserExercise map complete")

    map_tab_js = 'function () { emit( this._id.split("|")[0], 1 ) }' 

    for (min_vid, min_ex) in thresholds:
        desc = "min_vid=%d; min_ex=%d" % (min_vid, min_ex)
        g_logger.info(desc)
        query = {'value.exercises': {'$gte': min_ex}, 
                 'value.videos': {'$gte': min_vid}}
        mongo_db['vid_ex_users'].map_reduce(
                                    code(map_tab_js), 
                                    code(mongo_util.reduce_sum_js), 
                                    out="temp_monthly", 
                                    query=query)
        mongo_util.MongoUtil(db_name).copy_collection_into(
                "temp_monthly", output_collection, 
                add_flags={'tag': desc}, rename_id='month')


def monthly_badge_awards(output_collection):
    """Count users earning each badge for each month."""

    # First, map from UserBadge, keyed by badge_name|YYYY-MM|user
    map = """
    function () {
        var month = this.date.getMonth()+1;
        var date_key = (this.date.getYear()+1900) 
                     + "-" + (month < 10 ? "0"+month : month);
        var key = this.badge_name + "|" + date_key + "|" + this.user;
        emit( key, 1 );
    } 
    """
    reduce = mongo_util.reduce_identity_js
    mongo_db['UserBadge'].map_reduce(
            code(map), code(reduce), out="temp_monthly")
    
    # Next, aggregate months and output sums, keyed by badge_name|YYYY-MM
    map = """
    function () { 
        emit( this._id.split("|")[0] + "|" + this._id.split("|")[1], 1 ); 
    }
    """
    reduce = mongo_util.reduce_sum_js
    mongo_db['temp_monthly'].map_reduce(
            code(map), code(reduce), out="temp_monthly_2")
    
    # Finally, walk the result set and insert into output_collection 
    # in the proper form (see file docstring)
    for doc in mongo_db['temp_monthly_2'].find():
        (tag, month) = doc['_id'].split('|')
        newdoc = {'tag': 'badge(%s)' % tag, 
                  'month': month, 
                  'value': doc['value']}
        mongo_db[output_collection].insert(newdoc)
    
    mongo_db.drop_collection('temp_monthly')
    mongo_db.drop_collection('temp_monthly_2')


def monthly_accounts(output_collection):
    """Count new UserData objects created each month.
    
    Email-registered and phantom users are counted separately.  The 
    determination of whether each user is a phantom uses an assumption 
    that phantoms have the string 'nouserid' is in the user_id property.
    """

    map = '''
    function () {
        var month = this.joined.getMonth()+1;
        var date_key = (this.joined.getYear()+1900) + "-" 
                     + (month < 10 ? "0"+month : month);
        if (this.user_id.indexOf('nouserid') !== -1) {
            emit( date_key + "|phantom", 1)
        } else {
            emit(date_key + "|email", 1)
        }
    } 
    '''
    reduce = mongo_util.reduce_sum_js

    query = {'user_id': {'$exists': True}}
    mongo_db['UserData'].map_reduce(
        code(map), 
        code(reduce), 
        out="temp_registrations", 
        query=query) 
    
    for doc in mongo_db['temp_registrations'].find():
        (tag, month) = doc['_id'].split('|')
        newdoc = {'tag': 'registration(%s)' % tag, 
                  'month': month, 
                  'value': doc['value']}
        mongo_db[output_collection].insert(newdoc)
    
    mongo_db.drop_collection('temp_registrations')


def retention_cohort_analysis(min_group=None, max_group=None):
    """Tabluate return visits by join month and return month.
    
    The output data could be organized (see R code at bottom of this file)
    into a matrix of user counts with rows of join month and columns 
    of visit month.  
    
    In addtion, min_group and max_group provide parameter to constrain this
    function to only consider users within a coached group of size within
    those bounds. 

    min_group is the minimum size of the group coached by the users coach.
    max_group is the maximum size of the group allowed.
    
    NOTE: the output of this function would be more correct if based off of 
    ProblemLog/VideoLog (vs UserExercise/UserVideo), but was done this 
    way for data availability and computation tractability reasons.
    """

    # TODO(jace): since I'm re-using this, it shouldn't live in _examples, 
    # but I'm punting until we know what the future of mongo at KA is 
    mongo_examples.generate_cohort_maps()
    # load the map of cohort users, so we can cross-section by that if we want
    group_sizes = mongo_util.MongoUtil(db_name).load_collection_as_map(
                                                'user_max_cohort_size', 'user')
    
    # keyed by month, values are of the form {'visits': visits, 'users': users}
    cohorts = {}
    
    # walk through registered (non-phantom) users
    count = 0
    query = {'user_id': {'$exists': True, 
                         '$not': re.compile('/.*nouserid.*/')}}
    for user_data in mongo_db['UserData'].find(query):
        user = user_data['user']
        
        group_size = None
        if user in group_sizes:
            group_size = group_sizes[user]['max_cohort_size'] 
        
        if min_group is not None and (
                group_size is None or group_size < min_group):
            continue
        if max_group is not None and (
                group_size is not None and group_size > max_group):
            continue
              
        cohort_month = user_data['joined'].strftime('%Y-%m')
        if cohort_month not in cohorts:
            cohorts[cohort_month] = {}

        # for each registered user, create a list days they have visited
        visits = [user_data['joined'].strftime('%Y-%m-%d')]

        def include_visit_date(dt):
            if dt is None:
                return
            
            date_string = dt.strftime('%Y-%m-%d')
            if date_string not in visits:
                visits.append(date_string)
        
        for user_video in mongo_db['UserVideo'].find({'user': user}):
            include_visit_date(user_video['last_watched'])
        
        for user_exercise in mongo_db['UserExercise'].find({'user': user}):
            include_visit_date(user_exercise['first_done'])
            include_visit_date(user_exercise['last_done'])
            include_visit_date(user_exercise['proficient_date'])
        
        # roll this user's list of visit dates up into the monthly cohort stats
        cohort = cohorts[cohort_month]
        visit_months = []

        for visit in visits:
            month = visit[:7]  # grab YYYY-MM
                
            if month not in cohort:
                cohort[month] = {'users': 0, 'visits': 0}
            
            cohort[month]['visits'] += 1
            
            if month not in visit_months:
                visit_months.append(month)
                cohort[month]['users'] += 1
                
        count += 1
        if count % 10000 == 0:
            g_logger.info("Processed %d users." % count)
                
    min_suffix = str(min_group)  
    max_suffix = str(max_group)
    out_collection = 'report_cohort_analysis_%s_%s' % (min_suffix, max_suffix)
    mongo_db.drop_collection(out_collection)
    
    for cohort_name, cohort in cohorts.iteritems():
        for month, month_stats in cohort.iteritems():
            doc = {'cohort': cohort_name, 
                   'month': month, 
                   'users': month_stats['users'], 
                   'visits': month_stats['visits']}
            mongo_db[out_collection].insert(doc)


def main():
    output_collection = 'report_monthly_counts' 
    mongo_db.drop_collection(output_collection) 

    monthly_video_exercise_activity(output_collection)
    monthly_accounts(output_collection)
        
    retention_cohort_analysis()
    retention_cohort_analysis(min_group=1)
    retention_cohort_analysis(min_group=10)
    retention_cohort_analysis(min_group=1, max_group=3)


if __name__ == '__main__':
    main()
