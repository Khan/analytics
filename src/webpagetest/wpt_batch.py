#!/usr/bin/python2.6
#
# Copyright 2010 Google Inc. All Rights Reserved.

"""The script running one-off WebPageTest bulk testing.

This script makes use of the APIs in wpt_batch_lib.py to perform bulk
WebPageTest testing. Please vist http://code.google.com/p/webpagetest/source/browse/wiki/InstructionCommandlineTool.wiki for a detailed instrunction on how to use it.

Usage:
  wpt_batch.py -s the_url_of_your_wpt_server -i a/b/urls.txt -f c/d/result

  The script will perform WebPageTest for each URL in a/b/urls.txt on the
  given WebPageTest server and save the result in XML format into c/d/result
  directory. The resulting XML file is named as URL.test_id.xml. For more
  options to configure your tests, please type "wpt_batch.py -h".

Notice:
  The public instance of WebPageTest server (i.e., http://www.webpagetest.org/)
  is not allowed for the batch processing by default for security reason. This
  tool is intented for your own WebPageTest intance. If you really want to run
  it on public instance, please email to pmeenan@gmail.com to request an API
  key.
"""

__author__ = 'zhaoq@google.com (Qi Zhao)'

import logging
import optparse
import os
import sys
import time

import wpt_batch_lib


def BuildFileName(url):
    """Construct the file name from a given URL.

    Args:
      url: the given URL

    Returns:
      filename: the constructed file name
    """
    filename = url.strip('\r\n\t \\/')
    filename = filename.replace('http://', '')
    filename = filename.replace(':', '_')
    filename = filename.replace('/', '_')
    filename = filename.replace('\\', '_')
    filename = filename.replace('%', '_')
    return filename


def SaveTestResult(output_dir, url, test_id, extension, content):
    """Save the result of a test into a file on disk.

    Args:
      output_dir: the directory to save the result
      url: the associated URL
      test_id: the ID of the test
      extension: 'csv' or 'xml'
      content: the string of test result

    Returns:
      None
    """
    basename = BuildFileName(url)
    filename = os.path.join(output_dir,
                            '%s.%s.%s' % (basename, test_id, extension))
    output = open(filename, 'wb')
    output.write(content)
    output.close()


def StartBatch(options, verbose=False):
    """Set off some urls to be processed, and return the id-to-url map."""
    test_params = {'f': 'xml',
                   'private': 1,
                   'priority': 6,
                   'video': options.video,
                   'fvonly': options.fvonly,
                   'runs': options.runs,
                   'location': options.location,
                   'mv': options.mv
                  }
    if options.connectivity == 'custom':
        test_params['bwOut'] = options.bwup,
        test_params['bwIn'] = options.bwdown,
        test_params['latency'] = options.latency,
        test_params['plr'] = options.plr,
        test_params['location'] = options.location + '.custom'
    else:
        test_params['location'] = options.location + '.' + options.connectivity

    if options.tcpdump:
        test_params['tcpdump'] = options.tcpdump
    if options.script:
        test_params['script'] = open(options.script, 'rb').read()
    if options.key:
        test_params['k'] = options.key

    if isinstance(options.urlfile, list) or isinstance(options.urlfile, tuple):
        requested_urls = options.urlfile
    else:
        requested_urls = wpt_batch_lib.ImportUrls(options.urlfile)
    id_url_dict = wpt_batch_lib.SubmitBatch(requested_urls, test_params,
                                            options.server)
    if verbose:
        print 'Submitted %s urls to be tested' % len(requested_urls)
        sys.stdout.flush()

    submitted_urls = set(id_url_dict.values())
    for url in requested_urls:
        if url not in submitted_urls:
            logging.warn('URL submission failed: %s', url)

    return id_url_dict


def FinishBatch(id_url_dict, server, outputdir, csv_output, verbose=False):
    """Waits for the server to give results for queries from StartBatch.

    The main reason to separate these out is that multiple calls to
    StartBatch can be handled by a single call to FinishBatch (so the
    work of multiple batches can be done in parallel).  For this to work,
    though, all batches must use the same server and outputdir.
    """
    pending_test_ids = id_url_dict.keys()
    if outputdir and not os.path.isdir(outputdir):
        os.mkdir(outputdir)
    retval = {}
    while pending_test_ids:
        # TODO(zhaoq): add an expiring mechanism so that if some tests
        # are stuck too long they will reported as permanent errors
        # and while loop will be terminated.

        id_status_dict = wpt_batch_lib.CheckBatchStatus(pending_test_ids,
                                                        server_url=server)
        completed_test_ids = []
        for test_id, test_status in id_status_dict.iteritems():
            # We could get 4 different status codes with different meanings
            # as follows:
            # 1XX: Test in progress
            # 200: Test complete
            # 4XX: Test request not found
            if int(test_status) >= 200:
                pending_test_ids.remove(test_id)
                if test_status == '200':
                    completed_test_ids.append(test_id)
                else:
                    logging.warn('Tests failed with status $s: %s',
                                 test_status, test_id)

        if csv_output:
            test_results = wpt_batch_lib.GetCSVResult(completed_test_ids,
                                                      server_url=server)
        else:
            test_results = wpt_batch_lib.GetXMLResult(completed_test_ids,
                                                      server_url=server)
        result_test_ids = set(test_results.keys())
        for test_id in completed_test_ids:
            if test_id not in result_test_ids:
                logging.warn('The results failed to retrieve: %s', test_id)

        for test_id, result in test_results.iteritems():
            if verbose:
                print 'Done with %s' % str(id_url_dict[test_id])
                sys.stdout.flush()
            retval[id_url_dict[test_id]] = (test_id, result)
            if outputdir and csv_output:
                SaveTestResult(outputdir, id_url_dict[test_id], test_id, 'csv',
                               '\n'.join(result) + '\n')
            elif outputdir:
                SaveTestResult(outputdir, id_url_dict[test_id], test_id, 'xml',
                               result.toxml('utf-8'))
        if pending_test_ids:
            time.sleep(10)

    return retval


def RunBatch(options, verbose=False):
    """Run one-off batch processing of WebpageTest testing.

    Arguments:
        options: taken from GetOptions().  However, you may munge them
          manually to get the following special behavior not possible
          from just calling this script from the commandline:
        options.urlfile: if you set this to a list, it will be taken
          as the list of urls, rather than a filename to read urls
          from.
        verbose: print diagnostics as the testing goes along.

    Returns:
      A map from url to webpagetest results (as a minidom DOM object).
    """
    id_url_dict = StartBatch(options, verbose)
    return FinishBatch(id_url_dict, options.server, options.outputdir,
                       options.csv, verbose)


def GetOptions(argv=sys.argv[1:]):
    """Read options from the commandline and return an options struct."""
    class PlainHelpFormatter(optparse.IndentedHelpFormatter):
        def format_description(self, description):
            if description:
                return description + '\n'
            else:
                return ''

    option_parser = optparse.OptionParser(
        usage='%prog [options]',
        formatter=PlainHelpFormatter(),
        description='')

    # Environment settings
    option_parser.add_option('-s', '--server', action='store',
                             default='http://your_wpt_site/',
                             help='the wpt server URL')
    option_parser.add_option('-i', '--urlfile', action='store',
                             default='./urls.txt', help='input URL file')
    option_parser.add_option('-f', '--outputdir', action='store',
                             default='./result',
                             help='output directory ("" to suppress output)')

    # Test parameter settings
    help_txt = 'set the connectivity to pre-defined type: '
    help_txt += 'DSL, Dial, Fios and custom (case sensitive). '
    help_txt += 'When it is custom, you can set the customized connectivity '
    help_txt += 'using options -u/d/l/p.'
    option_parser.add_option('-k', '--key', action='store', default='',
                             help='API Key')
    option_parser.add_option('-y', '--connectivity', action='store',
                             default='DSL', help=help_txt)
    option_parser.add_option('-u', '--bwup', action='store', default=384,
                             help='upload bandwidth of the test')
    option_parser.add_option('-d', '--bwdown', action='store', default=1500,
                             help='download bandwidth of the test')
    option_parser.add_option('-l', '--latency', action='store', default=50,
                             help='rtt of the test')
    option_parser.add_option('-p', '--plr', action='store', default=0,
                             help='packet loss rate of the test')
    option_parser.add_option('-v', '--fvonly', action='store', default=1,
                             help='first view only')
    option_parser.add_option('-t', '--tcpdump', action='store_true',
                             help='enable tcpdump')
    option_parser.add_option('-c', '--script', action='store',
                             help='hosted script file')
    option_parser.add_option('-a', '--video', action='store', default=0,
                             help='capture video')
    option_parser.add_option('-r', '--runs', action='store', default=9,
                             help='the number of runs per test')
    option_parser.add_option('-o', '--location', action='store',
                             default='Test', help='test location')
    option_parser.add_option('-m', '--mv', action='store', default=1,
                             help='video only saved for the median run')
    option_parser.add_option('--csv', action='store_true',
                             help='store the data as csv files, not xml')

    options, args = option_parser.parse_args(argv)
    return options


def main():
    RunBatch(GetOptions())

if __name__ == '__main__':
    main()
