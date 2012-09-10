#!/usr/bin/env python

"""Small wrapper monitors the success or failure of steps for a 
jobflow_id, and sends email about the result.
"""

USAGE = """usage: %prog [jobflow_id]
"""

import sys

import emr
import notify


def monitor_jobflow(jobflow_id):
    status = emr.wait_for_completion(jobflow_id)
    
    listing = emr.list_steps(jobflow_id)
    jobname = jobflow_id
    heading = listing.split("\n")[0]
    # there just happens to be a fixed number of characters (85) in the 
    # output of the 'elastic-mapreduce --list' command before the jobname
    if len(heading) > 85:
        jobname += ": " + heading[85:]

    subject = "Jobflow status = %s (%s)" % (status, jobname)
    
    # Until we get more confident, always send email, even on success
    notify.send_email(subject, listing)
    failures = ["FAILED", "CANCELLED", "TERMINATED"]
    if any(s in listing for s in failures):
        notify.send_hipchat(subject) 

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print >> sys.stderr, USAGE
        sys.exit(-1)
    monitor_jobflow(sys.argv[1])
