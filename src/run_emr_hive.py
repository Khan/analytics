#!/usr/bin/env python

"""
A wrapper around the AWS elastic-mapreduce ruby script to run Hive scripts with
defaults that we like to use.
"""


USAGE = """usage: %prog [options] script_name \\
           [script_option_1 [script_option_2 ... ]]

Runs Hive scripts on Elastic Mapreduce with defaults that we like.

Script options (referenced by ${} in Hive scripts) should be specified in
a key=value format.

Example:
    %prog 's3://ka-mapreduce/code/hive/insert_topic_attempts.q' dt='2012-06-21'
"""

# TODO(david): Allow using spot instances to save money
# TODO(david): Have some way to run jobs that have dependencies, eg. by adding
#     steps to a jobflow


import optparse
import subprocess
import sys


def run_elastic_mapreduce(hive_script, job_name, script_args, options):
    """Run a hive script on Amazon Elastic Mapreduce.

    hive_script - location of the Hive script to execute on S3
    job_name - Human-readable description of the jobflow.
    script_args - dict of arguments to pass to the Hive script
    options - dict of command line options
    """
    script_args = dict({
        'INPATH': 's3://ka-mapreduce/entity_store',
        'OUTPATH': 's3://ka-mapreduce/tmp/',
    }, **script_args)

    # Can't quote argument values because they are literally inserted into Hive
    # scripts
    script_args = [['--args', '-d,%s=%s' % (arg[0], arg[1])] for arg in
            script_args.iteritems()]
    script_args = sum(script_args, [])  # Shallow flatten

    args = ['elastic-mapreduce', '--create',
        '--name', job_name,
        '--log-uri', options.log_uri,
        '--num-instances', options.num_instances,
        '--master-instance-type', options.master_instance_type,
        '--slave-instance-type', options.slave_instance_type,
        '--hive-script', '--arg', hive_script,
        '--args', '-i,"s3://ka-mapreduce/code/hive/ka_hive_init.q"',
    ]
    args.extend(script_args)
    args = [str(arg) for arg in args]

    # TODO(david): Echo back more things, like the hive script and the job name
    subprocess.call(args)


def parse_command_line_args():
    parser = optparse.OptionParser(USAGE)
    parser.add_option('--name', type='string', default=None,
        help='The name of the job flow being created')
    parser.add_option('--num_instances', type='int', default=2,
        help='Number of instances (machines) to use (default: %default)')
    parser.add_option('--master_instance_type', type='string',
        default='m1.small',
        help='The type of master instance to launch (default: %default')
    parser.add_option('--slave_instance_type', type='string',
        default='m1.large',
        help='The type of slave instance to launch (default: %default')
    parser.add_option('--log_uri', type='string',
        default='s3://ka-mapreduce/logs/',
        help='Location in S3 to store logs (default: %default)')

    options, args = parser.parse_args()
    if len(args) < 1:
        print >> sys.stderr, USAGE
        sys.exit(-1)

    return options, args


def main():
    options, args = parse_command_line_args()

    hive_script = args[0]
    script_args = dict(arg.split('=') for arg in args[1:])

    job_name = options.name or (
            'Hive script %s, args %s' % (hive_script, script_args))

    run_elastic_mapreduce(hive_script, job_name, script_args, options)


if __name__ == '__main__':
    main()
