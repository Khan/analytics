import random
import re
import sys

import pymongo

import util


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

def get_connection(mongo_server_name, config_location):
    """Return a pymongo.Connection to the named server.
    
    NOTE: the mongo_server_name is not the hostname of the machine or 
    the name of EC2 instance, it is the name given to the mongo server in 
    the main analytics config file, the location of which is 
    the second argument.
    """
    config = util.load_unstripped_json(config_location)
    server_config = config['servers']['mongo'][mongo_server_name]

    host = server_config['host']
    port = server_config['port']

    return pymongo.Connection(host, port)


def get_db(db_name, config_location):
    """Return a pymongo Database reference as configured in 'config'."""
    config = util.load_unstripped_json(config_location)
    db_config = config['databases']['mongo'][db_name]

    server_name = db_config['server']
    db_name = db_config['database']
    
    return get_connection(server_name, config_location)[db_name]


class MongoUtil() :
    """ Utility class that aids common uses of MongoDB data. """
    def __init__(self, dbname='testdb', host='localhost', port=27017):
        """Constructs a connection to specified mongo server.
        
        NOTE: Please avoid connecting to the mongo servers directly in 
        this way unless you have good reason.  Better to call get_connection()
        and get_db() which looks up a server's location by name. 
        """
        
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
