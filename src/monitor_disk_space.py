"""Script to check disk space usage and send alerts if too high.

Intended to be added to a crontab, e.g., 
0 1,13 0 0 0 python monitor_disk_space.py analytics 90
"""

import subprocess
import sys

import notify

USAGE = "%s HOSTNAME [percent usage threshold]" % sys.argv[0]

if len(sys.argv) < 2 or len(sys.argv) > 3:
    exit(USAGE)

hostname = sys.argv[1]

# set the usage alert limit.  default to 90%
threshold = int(sys.argv[2]) if len(sys.argv) == 3 else 90

# run 'df -h' an capture the output lines
df_output = subprocess.check_output(["df", "-h"])

lines = df_output.split("\n")
# filter for filesystems we care about
lines = [line for line in lines if line.startswith("/")]

warn = False
for line in lines:
    print line
    # grab a string percentage of usage, e.g., '78%'
    use_pct = line.split()[4]
    # convert to a number
    use_pct = int(use_pct[:-1])
    if use_pct > threshold:
        warn = True
        break

if warn:
    message = "WARNING: disk space low on machine '%s'" % hostname
    print >> sys.stderr, message
    print >> sys.stderr, df_output
    
    notify.send_hipchat(message)
    notify.send_email("WARNING: low disk space", message + "\n\n" + df_output)