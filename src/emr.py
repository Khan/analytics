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

import optparse
import subprocess
import sys
import time


def popen_results(args):
    proc = subprocess.Popen(args,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return proc.communicate()  # returns a tuple of (stdoutdata, stderrdata)


def script_arg_list(script_args):
    """Take a dictionary of Hive script args, merge in our usual *PATH
    args, and return in a flattened array as prefered by subprocess.Popen.
    """
    script_args = dict({
        'INPATH': 's3://ka-mapreduce/entity_store',
        'OUTPATH': 's3://ka-mapreduce/tmp/',
    }, **script_args)

    # Can't quote argument values because they are literally inserted
    # into Hive scripts
    script_args = [['--args', '-d,%s=%s' % (arg[0], arg[1])] for arg in
            script_args.iteritems()]
    script_args = sum(script_args, [])  # Shallow flatten

    return script_args


def create_hive_cluster(job_name, options, hive_script=None, script_args={}):
    """Creates a new Hive cluster on EMR and returns the jobflow_id.

    Note that we are not using the --alive option to be conservative, so
    you need to quickly add compute steps before setup completes, as the
    cluster will terminate when there are no steps.

    Arguments:
    job_name - Human-readable description of the jobflow.
    options - dict of options corresponding the OptionParser options below.
              For any option not specified in this dict, the OptionParser
              default is used.
    hive_script - Optional S3 location of a Hive script to execute.
    script_args - Optional dict of arguments to pass to the Hive script.

    Return value is the jobflow_id if successful, otherwise None.
    """

    options, overrides = get_default_options(), options
    options.update(overrides)

    args = ['elastic-mapreduce', '--create',
        '--name', job_name,
        '--log-uri', options['log_uri'],
        '--num-instances', options['num_instances'],
        '--master-instance-type', options['master_instance_type'],
        '--slave-instance-type', options['slave_instance_type']
        ]

    if hive_script:
        args.extend([
            '--args', '-i,"s3://ka-mapreduce/code/hive/ka_hive_init.q"',
            '--hive-script', '--arg', hive_script
            ])

        args.extend(script_arg_list(script_args))

    args = [str(arg) for arg in args]

    stdout_data = popen_results(args)[0].strip()
    # On success, the output is similar to 'Created job flow j-P2BQRK6WO2NP'
    jobflow_id = stdout_data.split(' ')[-1]
    if jobflow_id.startswith("j-"):
        print "Started  jobflow [%s]" % jobflow_id
    else:
        print >>sys.stderr, ("Couldn't determine jobflow id from [%s]."
                % stdout_data)
        jobflow_id = None

    return jobflow_id


def add_hive_step(jobflow_id, options, 
                  hive_script, script_args={},
                  step_name=None):
    """Add a Hive jobflow step to the specififed jobflow.

    Arguments:
    joblflow_id - Usually a return value from a previous call to create_cluster
    options - dict of options corresponding the OptionParser options below.
              For any option not specified in this dict, the OptionParser
              default is used.
    hive_script - S3 location of the Hive script to execute.
    script_args - Optional dict of arguments to pass to the Hive script.

    Returns the stdout of the elastic-mapreduce command.
    """
    options, overrides = get_default_options(), options
    options.update(overrides)

    if not step_name:
        script_base_name = hive_script.split("/")[-1].split(".")[0] 
        step_name = script_base_name + ": " + str(script_args)

    args = ['elastic-mapreduce',
            '--jobflow', jobflow_id,
            '--hive-script', 
            '--step-name', step_name,
            '--arg', hive_script,
            '--args', '-i,"s3://ka-mapreduce/code/hive/ka_hive_init.q"'
            ]

    args.extend(script_arg_list(script_args))

    args = [str(arg) for arg in args]

    stdout_data = popen_results(args)[0].strip()

    print stdout_data  # echo to stdout
    return stdout_data


def list_steps(jobflow_id=None):
    """Call `elastic-mapreduce --list`, and return the output as a string."""
    args = ['elastic-mapreduce', '--list']
    if jobflow_id:
        args.extend(['--jobflow', jobflow_id])

    return popen_results(args)[0].strip()


def wait_for_completion(jobflow_id, sleep=60):
    """Use polling to wait until a jobflow is complete and report status."""

    complete = False
    while not complete:

        listing = list_steps(jobflow_id)
        if not listing:
            raise Exception("Could not find listing for jobflow='%s'"
                    % jobflow_id)
        job_info = listing.split("\n")[0]
        job_status = job_info.split()[1]

        if job_status not in ['STARTING', 'RUNNING', 'SHUTTING_DOWN']:
            # job complete, probably with the status
            # 'COMPLETE', 'FAILED' or 'TERMINATED'
            complete = True

        time.sleep(sleep)

    return job_status


def get_option_parser():
    parser = optparse.OptionParser(USAGE)
    parser.add_option('--name', type='string', default=None,
        help='The name of the job flow being created')
    parser.add_option('--num_instances', type='int', default=3,
        help='Number of instances (machines) to use (default: %default)')
    parser.add_option('--master_instance_type', type='string',
        default='m1.small',
        help='The type of master instance to launch (default: %default')
    parser.add_option('--slave_instance_type', type='string',
        default='m2.xlarge',
        help='The type of slave instance to launch (default: %default')
    parser.add_option('--log_uri', type='string',
        default='s3://ka-mapreduce/logs/',
        help='Location in S3 to store logs (default: %default)')

    return parser


def get_default_options():
    # pass the option parsers an empty list for command line args,
    # so it will return all the default values
    options = get_option_parser().parse_args([])[0]
    return vars(options)  # convert to a plain old dictionary


def parse_command_line_args():
    parser = get_option_parser()
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

    create_hive_cluster(job_name, vars(options), hive_script, script_args)


if __name__ == '__main__':
    main()
