#!/usr/bin/env python

"""A script which uses SSH to run MySQL commands on a Hive cluster.

This abstracts away querying information about Hive tables from any Python
environment that has SSH credentials to access a Hive cluster's masternode.
Hive uses a MySQL instance to store meta data about tables and it's assumed
the master node specified has up to date information on the Hive tables
being queries.

Example uses:
  # Returns the S3 location of a table
  $ ./hive_mysql_connector.py ka-hive table_location <table_name>

  # Returns the column names and types for a table
  $ ./hive_mysql_connector.py --ssh_keyfile ~/.ssh/analytics.pem \
         table_columns <table_name>
"""

import optparse
import subprocess
import sys


_hive_hostname = None
_ssh_keyfile = None


def configure(hive_hostname, ssh_keyfile=None):
    """Configures the connection to the Hive masternode for the MySQL queries.
    
    Arguments:
        hive_hostname - the IP or hostname of the Hive masternode.
        ssh_keyfile - the location of the SSH key to use. If unspecified, will
            not provide a key location in the SSH command (and will rely on
            $HOME/.ssh/config to have the proper information for the host)
    """
    global _hive_hostname, _ssh_keyfile
    _hive_hostname = hive_hostname
    _ssh_keyfile = ssh_keyfile

    
def is_configured():
    return bool(_hive_hostname)


def _popen_results(args):
    proc = subprocess.Popen(args, stdout=subprocess.PIPE)
    return proc.communicate()[0]


def _run_mysql_query_over_ssh(query):
    """Runs the MySQL query over SSH.
    Note that query will be wrapped in single quotes, so it must be semi-sane.
    """

    if not is_configured():
        raise Exception("Connection to Hive master not configured yet.")
    
    global _hive_hostname, _ssh_keyfile

    base_command = ['ssh', _hive_hostname]
    if _ssh_keyfile:
        base_command + ['-i', _ssh_keyfile]

    # Encase the query in quotes.
    query = "'%s'" % query
    raw_results = _popen_results(base_command + ['sudo', 'mysql', 'hive_081',
                                                 '-e', query])
    if not raw_results:
        return []
    
    raw_results = raw_results.split("\n")
    if len(raw_results) < 1:
        # Bad results? The top header should always be the column names of 
        # the result set.
        return []
    return [tuple(row.strip().split("\t")) for row in raw_results[1:] if row]


def get_table_location(table_name):
    """Returns a single string for the table location, or None if not found."""
    args = {
        'table_name': table_name
    }
    query = """
        SELECT meta.LOCATION
        FROM TBLS t
        INNER JOIN SDS meta
        ON t.SD_ID = meta.SD_ID
        WHERE t.TBL_NAME = "%(table_name)s";
        """ % args
    results = _run_mysql_query_over_ssh(query)
    if results:
        return results[0][0]


def get_table_columns(table_name):
    """Returns an ordered list of tuples for the columns in the table.
    Each tuple has the info on a column in (name, type, comment) format, where
    each item is a string and comment may be string "NULL"
    """
    args = {
        'table_name': table_name
    }
    query = """
        SELECT c.COLUMN_NAME, c.TYPE_NAME, c.COMMENT
        FROM COLUMNS_V2 c
        LEFT OUTER JOIN TBLS t
        ON t.TBL_ID = c.CD_ID
        WHERE t.TBL_NAME = "%(table_name)s"
        ORDER BY c.INTEGER_IDX;
        """ % args
    return _run_mysql_query_over_ssh(query)


command_map = {
    'table_location': get_table_location,
    'table_columns': get_table_columns,
}


def run_command(command, args):
    command = command_map.get(command)
    if not command:
        raise Exception("Unknown command %s" % command)

    results = command(*args)
    print str(results)


# TODO(benkomalo): print command args as well?
USAGE = """%prog <options> [hive_hostname] [command] [args]

Available commands:
""" + '\n'.join(['\t%s' % command for command in command_map.keys()])

if __name__ == '__main__':
    parser = optparse.OptionParser(USAGE)
    parser.add_option('--ssh_keyfile',
        help='A location of an SSH pem file to use for SSH connections ' +
             'to the specified Hive machine')

    options, args = parser.parse_args()
    if len(args) < 2:
        print >> sys.stderr, USAGE
        sys.exit(-1)

    configure(args[0], options.ssh_keyfile)
    run_command(args[1], args[2:])
    
