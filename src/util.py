#!/usr/bin/env python
""" 
Utility functions and classes for Khan Academy analytics sorted alphabetically
"""


import datetime
import errno
import hashlib
import json
import logging
import os
import re
import sys
import time

from pymongo.errors import AutoReconnect


class LoopProgressLogger():
    iteration_counts = {}
    def log(self, msg='', mod=100, tag='', to_stderr=True):
        if tag not in self.iteration_counts:
            self.iteration_counts[tag] = 0
        self.iteration_counts[tag] += 1
        if self.iteration_counts[tag] % mod == 0:
            msg_prefix = "loop %s progress:  %d " % (tag, self.iteration_counts[tag])
            if to_stderr:
                print >> sys.stderr, msg_prefix + msg
            else:
                print msg_prefix + msg

class Params(object):
    """ 
    Utility class to group a bunch a named params
    E.g., params = Params(datum=y, squared=y*y, coord=x)
    """
    def __init__(self, **kwds):
        self.__dict__.update(kwds)


def add_dict(from_dict, to_dict):
    """ 
    useful for aggregating stats from one dictionary into another,
    where the dictionary values are also dictionaries of named stats
    """
    for key, stats in from_dict.iteritems():
        if key not in to_dict:
            to_dict[key] = stats.copy()
        else:
            for stat_name, stat in stats.iteritems():
                to_dict[key][stat_name] += stat


def clean_datetime_str(origstr) :
    """ 
    this function takes a string, and makes sure there 
    is a space (and not a 'T') between the date and time
    """
    assert len(origstr)==19 or (len(origstr)==20 and origstr[-1]=='Z'), (
         "Expected datetime string of format YYYY-MM-DD HH:MM:SS, with either a space or \
          a 'T' between the date and time and a 'Z' at the end. Instead, received: '%s'"
           % origstr)
    trailer = ''
    if len(origstr)==19:
        trailer = 'Z' 
         
    return origstr[:10] + ' ' + origstr[11:] + trailer


def db_decorator(max_tries, func):
    """Decorator for db functions. Attempts to handle errors by retrying."""
    def new_func(*args, **kwargs):
        tries = 0
        while True:
            try:
                tries += 1
                ret = func(*args, **kwargs)
            except AutoReconnect:
                if tries < int(max_tries):
                    time.sleep(3)
                    continue
                else:
                    print (
                        "Can't connect to mongo after %s tries" % (tries))
                    raise
            return ret
    return new_func


def dict_to_csv(dict, out_filename=None):
    
    original_stdout = sys.stdout
    if out_filename is not None:
        sys.stdout = open(out_filename, 'w')
    
    first_doc = True
    for key, row in dict.iteritems():
        fields = row.keys()
        if first_doc:
            print 'key,' + ','.join([str(field) for field in fields])
            first_doc = False
        print key + ',' + ','.join([str(row[field]) for field in fields])
    
    sys.stdout = original_stdout


def get_data_dir():
    """return the value of environment variable containing path to working data set"""
    return os.getenv( 'KA_DATA_DIR', "." )


def get_data_filename(kind):
    """ 
    this function takes a kinds as a string, and returns the path to the
    file storing that data
    """
    # TODO assert that the file exisits
    return get_data_dir() + '/' + kind + '.csv'


def get_logger():
    """Return an intialized logger"""
    logger = logging.getLogger('gae_download')
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('[%(levelname)s %(asctime)s] %(message)s')
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger


def global_file_replace(filename, substitutions):
    """ 
    this function reads the file, make the word subsitutions and writes back out to stdout
    E.g., global_file_replace( "myfile.csv", [ {'from':'[', 'to':'"['}, 
              {'from':']', 'to':']"'} ] )  
    """
    f = open( filename, 'rt' )
    for line in f:
        for s in substitutions:
            line.replace( s['from'], s['to'] )
        print line


def load_unstripped_json(config_file):
    """load json config file with comments"""
    with open(config_file) as f:
        content = f.read()
    return json.loads(re.sub(r"#.*\n", "", content))


def mkdir_p(path):
    """mkdir -p {path}"""
    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno == errno.EEXIST:
            pass
        else:
            raise

def passes_random_hash_filter(key_string, percentage_kept=.50):
    """
    allows you to pass in a string (like user_name, or exercise_name) 
    and it fill return True approximately percentage_kept % of the time.
    percentage_kept should be between 0.0 and 1.0
    """
    sig = hashlib.md5(key_string).hexdigest()
    sig_num = int(sig, base=16)
    return (sig_num % 100 <= percentage_kept*100)


def split2(origstr) :
    """ 
    this function takes a comma delimited string with extra quoting and splits 
    out only the actual values within the quotes.  E.g., 
    split2("[u'Computer Science', u'Physcis']") -> ['Computer Science', 'Physics']
    """
    vals = origstr.split("'")
    arr = []
    for i in range(1, len(vals), 2):
        arr.append(vals[i])
    return arr


def stdout_redirect_to_file_start(filename):
    outfile = None
    if filename!='':
        outfile = open(filename, "w")
        sys.stdout = outfile
    return outfile


def stdout_redirect_to_file_stop(outfile):
    if outfile is not None:
        outfile.close()
        sys.stdout = sys.__stdout__


def str_2_datetime (origstr):
    """ 
    this function takes a string representing a datetime in the 
    format commonly used by the CSV connector conversion 
    and returns a python datetime 
    """
    return datetime.datetime.strptime( clean_datetime_str(origstr), '%Y-%m-%d %H:%M:%SZ' ) 
