#!/usr/bin/env python
import datetime
import optparse
import subprocess
import sys

import emr


def run_hive_jobs(start_dt, end_dt, earliest_dt):
    jobname = "Growth Reporting (%s to %s)" % (start_dt, end_dt)
    jobflow = emr.create_hive_cluster(jobname, {})

    # TODO(jace): make sure the required data (ProblemLogs, etc)
    # is available before running these downstream summaries
    emr.add_hive_step(jobflow, {},
            hive_script='s3://ka-mapreduce/code/hive/user_daily_activity.q',
            script_args={"start_dt": start_dt, "end_dt": end_dt})
    emr.add_hive_step(jobflow, {},
            hive_script='s3://ka-mapreduce/code/hive/user_growth.q',
            script_args={"start_dt": earliest_dt, "end_dt": end_dt})
    emr.add_hive_step(jobflow, {},
            hive_script='s3://ka-mapreduce/code/hive/company_metrics.q',
            script_args={"start_dt": earliest_dt, "end_dt": end_dt})

    return jobflow, emr.wait_for_completion(jobflow)


def run_report_importer(hive_table, mongo_table):
    # TODO(jace): make report_importer truly callable as a library
    command = ('python /home/analytics/analytics/src/report_importer.py'
               '  --hive_init --drop'
               '  ka-hive %s report %s') % (hive_table, mongo_table)
    proc = subprocess.Popen(command.split(),
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    print proc.communicate()  # returns a tuple of (stdoutdata, stderrdata)


def main():
    parser = optparse.OptionParser()
    parser.add_option("-b", "--begindate", help="In format YYYY-MM-DD.")
    parser.add_option("-e", "--enddate", help="In format YYYY-MM-DD.")
    options, dummy = parser.parse_args()

    today = datetime.datetime.combine(datetime.date.today(), datetime.time())
    yesterday = today - datetime.timedelta(days=1)

    if options.begindate and options.enddate:
        start_date = options.begindate
        end_date = options.enddate
    else:
        start_date = yesterday.strftime("%Y-%m-%d")
        end_date = today.strftime("%Y-%m-%d")
    earliest_date = "2011-01-01"  # farthest back we've ever populated

    jobflow, status = run_hive_jobs(start_date, end_date, earliest_date)

    print "Jobflow %s ended with status %s." % (jobflow, status)
    if status != "COMPLETED":
        # if cronned, this will get sent as email
        print >>sys.stderr, emr.list_steps(jobflow)
        sys.exit(1)

    # Jobflow was successful, so transfer the data to
    # the Mongo reporting db used by dashboards
    run_report_importer("user_growth", "user_growth")
    run_report_importer("company_metrics", "company_metrics")


if __name__ == '__main__':
    main()
