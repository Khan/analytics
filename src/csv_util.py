#!/usr/bin/env python

""" This file provides a few functions useful for processing csv files """

import csv

def remove_columns ( infilename, outfilename, cols_to_remove ):
    """ 
    Reads in a file from infilename, removes columns at indices cols_to_remove, write back out to outfilename 
    """
    xcols = cols_to_remove
    xcols.sort()
    xcols.reverse()
    
    reader = csv.reader( open( infilename, 'rt' ), quotechar='"', quoting=csv.QUOTE_MINIMAL)
    writer = csv.writer( open( outfilename, 'wb' ), quotechar='"', quoting=csv.QUOTE_MINIMAL)

    for row in reader:
        vals = row
        for x in xcols :
            vals.pop( x )
        writer.writerow( vals )
     
def move_columns_to_front ( infilename, outfilename, cols_to_move ):
    """ 
    Reads in a file from infilename, print out a new file where the columns specified are the first columns (useful for passing to 'sort')
    """
    xcols = cols_to_move
    
    reader = csv.reader( open( infilename, 'rt' ), quotechar='"', quoting=csv.QUOTE_MINIMAL)
    writer = csv.writer( open( outfilename, 'wb' ), quotechar='"', quoting=csv.QUOTE_MINIMAL)

    for row in reader:
        vals = row
        i = 0
        for x in xcols :
            vals[i], vals[x] = vals[x], vals[i]
            i += 1
        writer.writerow( vals )
     

def cut ( infilename, outfilename, cols ):
    """ 
    Similar to the 'cut' linux command, extracts the columns at the specified indices and prints them out (to outfilename) 
    """
    reader = csv.reader( open( infilename, 'rt' ), quotechar='"', quoting=csv.QUOTE_MINIMAL)
    writer = csv.writer( open( outfilename, 'wb' ), quotechar='"', quoting=csv.QUOTE_MINIMAL)

    for row in reader:
        newvals = []
        for x in cols :
            newvals.append( row[x] )
        writer.writerow( newvals )
     
class CSVFieldIndexer:
    """
    This class is a utilitiy that can take a string that is the first/header row 
    of a CSV file, and will store the indices as attributes named as the fieldname. 
    """
    def __init__(self, header):
        fields = header.strip().split(',')
        for i in range(len(fields)):
            self.__dict__[fields[i]] = i
