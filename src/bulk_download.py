#!/usr/bin/env python
""" This script connects to appengine to download backups on the datastore."""


import datetime
import filecmp
import json
import multiprocessing
import optparse
import os
import sqlite3
import sys
import time
import traceback

sys.path.append(os.path.dirname(__file__) + "/../map_reduce/py")
import load_pbufs_to_hive


def echo_system(command):
    print command
    return os.system(command)


def filename(kind, config):
    return os.path.join(config['result_directory'], config['date'], kind)


def email_summary(config, results):
    subject = "KA backup summary"
    summary = "successes: %s\nfailures: %s" % (
        ', '.join(results['successes']), ', '.join(results['failures']))
    send_email(subject, config, body=summary)


def send_email_notification(kind, config, dowloaded, attempts):
    file_beginning = filename(kind, config)
    subject = ("KA backup download status notification: %s," +
        " download_success=%d, attempts=%d") % (
            file_beginning, dowloaded, attempts)
    command = "ls -1 %s*.log | xargs -d'\n' tail -n 500" % file_beginning
    send_email(subject, config, body_command=command)


def send_email(subject, config, body=None, body_command=None):
    if not config['emails'] or (not body and not body_command):
        return
    addresses = ' '.join(config['emails'])
    body_command = body_command or "echo '%s'" % body
    subject_date = subject + " (" + config['date'] + ")"
    command = "%s | mailx -s '%s' %s" % (body_command, subject_date, addresses)
    echo_system(command)


def attempt_download(kind, config, attempt_num):
    # s~appid necessary b/c of bug:
    # http://groups.google.com/group/google-appengine/browse_thread/thread/5f7ffd8d3146de2a/092f97bd2ec83bbd?show_docid=092f97bd2ec83bbd
    file_beginning = filename(kind, config)
    call_args = [config['appcfg'],
                 'download_data',
                 '--application=s~khan-academy',
                 '--url=%s' % config['appurl'],
                 '--filename=%s.dat' % file_beginning,
                 '--db_filename=%s.progress.dat' % file_beginning,
                 '--result_db_filename=%s.result.dat' % file_beginning,
                 '--log_file=%s.bulkloader.log' % file_beginning,
                 '--email=khanbackups@gmail.com',
                 '--num_threads=55',
                 '--batch_size=500',
                 '--bandwidth_limit=10000000',
                 '--rps_limit=15000',
                 '--http_limit=40',
                 '--kind=%s' % kind
                ]

    if 'password' in config and config['password']:
        call_args.append('--passin')
        command = 'nohup %s < %s > %s.console.log' % \
            (' '.join(call_args), config['password'],
             file_beginning + str(attempt_num))
    else:
        command = 'nohup %s > %s.console.log' % \
            (' '.join(call_args), file_beginning + str(attempt_num))

    print command
    exitcode = os.system(command)
    print "Exit code was %d." % exitcode
    return exitcode == 0 and \
        os.path.exists(file_beginning + '.dat') and \
        filecmp.cmp(file_beginning + '.dat', file_beginning + '.result.dat')


def robust_backup(kind, config, results):
    try:
        downloaded = False
        for i in range(0, config['max_tries']):
            downloaded = attempt_download(kind, config, i)
            if downloaded:
                break

        send_email_notification(kind, config, downloaded, i + 1)

        if downloaded:
            results['successes'].append(kind)
        else:
            results['failures'].append(kind)
            return

        file_beginning = filename(kind, config)
        # if download succeded, remove the duplicate copy
        if downloaded:
            echo_system("rm %s.result.dat" % file_beginning)
        # json the file
        if config['json']:
            jsonify_downloaded_file(kind, config)
        echo_system("gzip -f %s.dat" % file_beginning)
    except Exception:
        send_email("KA backup exception", config, body=traceback.format_exc())
        traceback.print_exc(file=sys.stdout)


def jsonify_downloaded_file(kind, config):
    file_beginning = filename(kind, config)
    dat_filename = "%s.dat" % file_beginning
    json_filename = "%s.json" % file_beginning

    sqlite_conn = sqlite3.connect(dat_filename, isolation_level=None)
    sqlstring = 'SELECT id, value FROM result'
    cursor = sqlite_conn.cursor()
    cursor.execute(sqlstring)
    f = open(json_filename, 'wb')
    for unused_entity_id, pb in cursor:
        doc = load_pbufs_to_hive.pb_to_dict(pb)
        json_str = json.dumps(doc)
        print >>f, "%s\t%s" % (doc['key'], json_str)
    f.close()
    sqlite_conn.close()

    echo_system("gzip -f %s" % json_filename)


def monitor(config, processes):
    """ Check to make sure the processes aren't hung"""

    for (process, kind) in processes:
        file = filename(kind, config)
        if process.is_alive() and os.path.exists(file):
            mtime = os.stat(file).st_mtime
            now = time.time()
            if now - mtime > 10 * 60:  # It's been longer than 10 minutes
                process.terminate()
                message = "At %s the process downloading '%s' hung" % \
                        (time.strftime("%a, %d %b %H:%M:%S"), kind)
                send_email("KA backup hung", config, body=message)
                print(message)


def main():
    parser = optparse.OptionParser(usage="%prog [options]",
        description="A robust/resumable script for downloading data " +
                    "from App Engine datastore.")
    parser.add_option("-c", "--config", default='bulk_download.json',
        help='Location of the config for this backup')
    parser.add_option("-d", "--date",
        default=datetime.datetime.now().strftime('%Y-%m-%d'),
        help='date YYYY-mm-dd')
    options, unused_options = parser.parse_args()

    with open(options.config) as f:
        config = json.load(f)
    config['date'] = options.date
    config['json'] = config.get('json', 0)

    results = {'successes': [], 'failures': []}
    path = os.path.join(config['result_directory'], config['date'])
    if not os.path.exists(path):
        os.makedirs(path)

    # create a set of processes executing robust_backup for each kind, and make
    # sure they write to their log at least every 10 minutes
    processes = []
    i = 0
    while i < len(config['kinds']):
        if len(multiprocessing.active_children()) < config['parallelism']:
            kind = config['kinds'][i]
            p = multiprocessing.Process(target=robust_backup,
                                        args=(kind, config, results))
            p.start()
            processes.append((p, kind))
            i += 1

        time.sleep(10)
        monitor(config, processes)

    while len(multiprocessing.active_children()) > 0:
        time.sleep(10)
        monitor(config, processes)

    email_summary(config, results)


if __name__ == '__main__':
    main()
