import pymongo
import bson

mongo = pymongo.Connection() 
mongo_db = mongo['kadb']

# helper function for later examples
def user_registration_type(user):
    # mmm... no promises this classification is actually right
    current_user = user['current_user']
    if 'facebookid' in current_user:
        return 'facebook'
    elif 'nouserid' in current_user:
        return 'phantom'
    elif '@' in current_user:
        return 'email'
    else:
        return 'google'


# Example 1:  a trivial query 
def simple_query_example():
    user = mongo_db['UserData'].find_one({'user':'sal@khancapital.com'})
    print user 


# Example 2:  compute the count of facebook, google, custom domain, and phantom users
def iterative_user_registration_types():
    counts = {'facebook':0, 'google':0, 'email':0, 'phantom':0}

    users = mongo_db['UserData'].find()
    for user in users:
        counts[user_registration_type(user)] += 1
        
    print counts


# Example 3:  same as the #2, but using map reduce instead of a cursor walk
map_js = '''
function () {
    if (this.current_user.indexOf("facebookid") >=0) {
        emit("facebook", 1);
    } else if (this.current_user.indexOf("nouserid") >=0) {
        emit("phantom", 1);
    } else if (this.current_user.indexOf("@") >=0) {
        emit("email", 1);
    } else {
        emit("google", 1);
    }
}
'''
reduce_js = '''
function (key, values) {
    var total = 0;
    for (var i = 0; i < values.length; i++) {
        total += values[i];
    }
    return total;
}
'''
def mapreduce_user_registration_types():
    map = bson.Code(map_js)
    reduce = bson.Code(reduce_js)
    result = mongo_db['UserData'].map_reduce(map, reduce, "registration_types")
    for doc in result.find():
        print doc


# Example 4:  Generate and store a new collection of coaches with their list of students
def generate_cohort_maps():

    if 'Coaches' in mongo_db.collection_names():
        return

    # create the coach -> studentlist map
    coaches = {}
    users = mongo_db['UserData'].find()
    for user_data in users:
        if user_data['user']=='':
            continue
        coach_list = user_data['coaches'] if 'coaches' in user_data else []
        for coach in coach_list:
            if coach not in coaches:
                coaches[coach] = []
            if user_data['user'] not in coaches[coach]:
                coaches[coach].append(user_data['user'])
    
    # store the coach -> student_list map 
    for coach in coaches:
        doc = {'coach':coach, 'num_students':len(coaches[coach]), 'students':coaches[coach]}
        mongo_db['Coaches'].insert(doc)
        
        
    # compute the size of the largest cohort each user is in
    user_cohort_size = {}
    for coach in coaches:
        for student in coaches[coach]:
            if student not in user_cohort_size:
                user_cohort_size[student] = len(coaches[coach])
            user_cohort_size[student] = max(len(coaches[coach]), user_cohort_size[student])
    print "cohort users: %d" % len(user_cohort_size)
    
    # store the user -> max_cohort_size map
    tablename = 'user_max_cohort_size'
    if tablename in mongo_db.collection_names():
        mongo_db.drop_collection(tablename)
    for user in user_cohort_size:
        doc = {'user':user, 'max_cohort_size':user_cohort_size[user]}
        mongo_db[tablename].insert(doc)
    mongo_db[tablename].ensure_index('user')
    

# Example 5:  Use a range query to count the couches with at least N students
def count_cohorts(min_size=10):
    count = mongo_db['Coaches'].find({"num_students": {"$gte":min_size}}).count()
    print "There are %d coaches with more than %d students." % (count, min_size)


# Example 6: Simple mapreduce summarize per user the number of exercises and videos started/completed
map_js = '''
  function() {
    emit( this.user, {attempts: 1, completions: this.proficient_date ? 1 : 0} );
  }
'''
reduce_js = '''
  function(key, values) {
    var result = {attempts: 0, completions: 0};

    values.forEach(function(value) {
      result.attempts += value.attempts;
      result.completions += value.completions;
    });

    return result;
  }
'''
def mapreduce_exercises_per_user():
    result = mongo_db['UserExercise'].map_reduce(bson.Code(map_js), bson.Code(reduce_js), "exercises_per_user")

map_js = '''
  function() {
    emit( this.user, {attempts: 1, completions: this.completed ? 1 : 0} );
  }
'''
def mapreduce_videos_per_user():
    result = mongo_db['UserVideo'].map_reduce(bson.Code(map_js), bson.Code(reduce_js), "videos_per_user")


# Example 6b
def stats_per_user():
    user_stats = {}
    stat_names = ['vids_started', 'vids_completed', 'exs_started', 'exs_completed']

    def init_stats_if_necessary(user, user_stats):
        if user not in user_stats:
            user_stats[user] = dict([(s,0) for s in stat_names])
             
    print "Loading UserExercise"
    count = 0
    user_exs = mongo_db['UserExercise'].find()
    for user_ex in user_exs:
        count += 1
        if count % 1000000 == 0:
            print "processed %d docs" % count
        init_stats_if_necessary(user_ex['user'], user_stats)
        user_stats[user_ex['user']]['exs_started'] += 1
        if user_ex['proficient_date'] is not None:
            user_stats[user_ex['user']]['exs_completed'] += 1
                
    print "Loading UserVideo"
    count = 0
    user_vids = mongo_db['UserVideo'].find()
    for user_vid in user_vids:
        count += 1
        if count % 1000000 == 0:
            print "processed %d docs" % count
        init_stats_if_necessary(user_vid['user'], user_stats)
        user_stats[user_vid['user']]['vids_started'] += 1
        if 'completed' in user_vid and user_vid['completed']:
            user_stats[user_vid['user']]['vids_completed'] += 1

    print "Aggregating results"
    
    cutoffs = [1,3,5,10,15,30,50,75,100,150,200]
    user_counts = [0] * len(cutoffs)
    
    for i in range(len(cutoffs)):

        sum_stats = [0] * len(stat_names)
        for user in user_stats:
            for s in range(len(stat_names)):
                if user_stats[user][stat_names[s]] > cutoffs[i]:
                    sum_stats[s] += 1

        print cutoffs[i] , 
        print ','.join([str(s) for s in sum_stats])



# Example 7 - daily ProblemLog count
map = '''
    function () {
        var month = this.time_done.getMonth()+1;
        var day = this.time_done.getDate();
        var date_key = (this.time_done.getYear()+1900) + "-" + (month < 10 ? "0"+month : month) + "-" + (day < 10 ? "0"+day: day) ;
        emit( date_key + "|" + "ALL", 1 );
        emit( date_key + "|" + this.exercise, 1)
    } 
'''
reduce = '''
function (key, values) {
    var total = 0;
    for (var i = 0; i < values.length; i++) {
        total += values[i];
    }
    return total;
}
'''
def daily_problem_count():
    result = mongo_db['ProblemLog'].map_reduce(bson.Code(map), bson.Code(reduce), "daily_problem_count")


if __name__ == '__main__':
    generate_cohort_maps()
    
            
 

