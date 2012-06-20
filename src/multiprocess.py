#!/usr/bin/env python
"""Provide utility to execute a list of shell commands in parallel."""

import multiprocessing 
import optparse
import os
import sys
import time


def run_command(command):
    os.system(command)


def multiprocess_list(command_list, num_processes):
    """Excecutes up to 'num_processes' shell commands in parallel."""
    
    while len(command_list) > 0:
        
        if len(multiprocessing.active_children()) < num_processes:
            print "Starting child process for command %s." % command_list[-1]
            p = multiprocessing.Process(target=run_command, 
                                        args=(command_list[0].strip(),))
            p.start()
            command_list.pop(0)
        
        time.sleep(5)  # just a safety allowing control-C if things go wrong


def multiprocess_file(file, num_processes):
    command_list = [command.strip() for command in file.readlines()]
    multiprocess_list(command_list, num_processes)


def main():
    parser = optparse.OptionParser(usage="%prog [options]", 
        description="Run a set of shell commands in parallel.")
    
    parser.add_option("-f", "--file", 
                      help="File of commands.  If not provided, commands are "
                           "read from stdin.")
    parser.add_option("-p", "--processes", 
                      help="The maximum number of parallel child processes to "
                           "run concurrently.", type="int")
    
    options, extra_args = parser.parse_args()

    if options.processes is None:
        print "-p <max # of concurrent processes> is a required argument."
        exit(1)
    
    if options.file is None:
        with open(options.file, 'r') as file:
            multiprocess_file(file, options.processes)
    else:
        multiprocess_file(sys.stdin, options.processes)


if __name__ == '__main__':
    main()

