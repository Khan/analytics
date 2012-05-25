import random
import re
import sys

import pymongo


# reuse this JS code for a reducer that just sums
reduce_sum_js = '''
function (key, values) {
    var total = 0;
    for (var i = 0; i < values.length; i++) {
        total += values[i];
    }
    return total;
}
'''
reduce_identity_js = '''
function (key, values) {
    return 1;
}
'''

class MongoUtil() :
    """ Utility class that aids common uses of MongoDB data. """
    def __init__(self, dbname='testdb', host='localhost', port=27017):
        self.connection = pymongo.Connection(host, port)
        self.db = self.connection[dbname]
        
    def load_collection_as_map(self, collection_name, key_name):
        result = {}
        docs = self.db[collection_name].find()
        for doc in docs:
            result[doc[key_name]] = doc
        return result

    def load_collection_as_list(self, collection_name):
        return [doc for doc in self.db[collection_name].find()]
    
    def copy_collection_into(self, from_collection, to_collection, add_flags=None, rename_id=None):
        for doc in self.db[from_collection].find():
            if add_flags is not None:
                doc.update(add_flags)
            if rename_id is not None:
                doc[rename_id] = doc['_id']
                del doc['_id']
            self.db[to_collection].insert(doc)

    def write_csv(self, collection_name, out_filename=None, property_names=None):
        
        original_stdout = sys.stdout
        if out_filename is not None:
            sys.stdout = open(out_filename, 'w')
        
        # if property_names are not specified, use the properties
        # from the first document (assumes all other docs will have these, too)
        if property_names is None:
            property_names = self.db[collection_name].find_one().keys()
            
        print ','.join(property_names)
        
        docs = self.db[collection_name].find()
        for doc in docs:
            print ','.join([str(doc[prop]) for prop in property_names])
        
        sys.stdout = original_stdout

    def get_registered_users(self, sample_pct=1.0):
        user_list = []
        for user_data in self.db['UserData'].find({'current_user':{'$not':re.compile('/.*nouserid.*/')}}):
            user = user_data['user']
            if user in ['None', '']:
                continue
            if sample_pct > 0.0 and sample_pct < 1.0 and random.random() < sample_pct:
                user_list.append(user)
        return user_list
