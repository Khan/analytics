#!/usr/bin/env python

"""Run pep8 on every non-blacklisted python file under the current directory.

The arguments to this script are passed verbatim to pep8.  The only
difference between this script and running pep8 directly is you don't
give filenames to this script; it figures them out automatically from
the current working directory.  It uses runlint_blacklist.txt to
exclude files from checking.
"""

# TODO(benkomalo): consolidate this with the main app via a subrepo

import cStringIO
import os
import sys

try:
    import pep8
except ImportError, why:
    sys.exit('FATAL ERROR: %s.  Install pep8 via "pip install pep8"' % why)
try:
    from pyflakes.scripts import pyflakes
except ImportError, why:
    sys.exit('FATAL ERROR: %s.  Install pyflakes via "pip install pyflakes"'
             % why)


_BLACKLIST_FILE = os.path.join(os.path.dirname(__file__),
                               'runlint_blacklist.txt')


# W291 trailing whitespace
# W293 blank line contains whitespace
# W391 blank line at end of file
_DEFAULT_PEP8_ARGS = ['--repeat',
                      '--ignore=W291,W293,W391']


def _parse_blacklist(blacklist_filename):
    """Read from blacklist filename and returns a set of the contents.

    Blank lines and those that start with # are ignored.

    Arguments:
       blacklist_filename: the name of the blacklist file

    Returns:
       A set of all the paths listed in blacklist_filename.
       These paths may be filenames *or* directory names.
    """
    retval = set()
    contents = open(blacklist_filename).readlines()
    for line in contents:
        line = line.strip()
        if line and not line.startswith('#'):
            retval.add(line)
    return retval


def _files_to_process(rootdir, blacklist):
    """Return a set of .py files under rootdir not in the blacklist."""
    retval = set()
    for root, dirs, files in os.walk(rootdir):
        # Prune the subdirs that are in the blacklist.  We go
        # backwards so we can use del.  (Weird os.walk() semantics:
        # calling del on an element of dirs suppresses os.walk()'s
        # traversal into that dir.)
        for i in xrange(len(dirs) - 1, -1, -1):
            if os.path.join(root, dirs[i]) in blacklist:
                del dirs[i]
        # Take the files that end in .py and are not in the blacklist:
        for f in files:
            if f.endswith('.py') and os.path.join(root, f) not in blacklist:
                retval.add(os.path.join(root, f))
    return retval


def _capture_stdout_of(fn, *args, **kwargs):
    """Call fn(*args, **kwargs) and return (fn_retval, fn_stdout_output_fp)."""
    try:
        orig_stdout = sys.stdout
        sys.stdout = cStringIO.StringIO()
        retval = fn(*args, **kwargs)
        sys.stdout.reset()    # so new read()/readlines() calls will return
        return (retval, sys.stdout)
    finally:
        sys.stdout = orig_stdout


class Pep8(object):
    """process() processes one file."""
    def __init__(self, all_files, pep8_args):
        pep8.process_options(pep8_args + list(all_files))
        self._num_errors = 0

    def _process_one_line(self, output_line, contents_lines):
        """If line is a 'error', print it and return 1.  Else return 0.

        pep8 prints all errors to stdout.  But we want to ignore some
        'errors' that are ok for us but cannot be suppressed via pep8
        flags, such as lines marked with @Nolint.  To do this, we
        intercept stdin and remove these lines.

        Arguments:
           output_line: one line of the pep8 error-output
           contents_lines: the contents of the file being linted,
              as a list of lines.

        Returns:
           1 (indicating one error) if we print the error line, 0 else.
        """
        bad_linenum = int(output_line.split(':', 2)[1])   # first line is '1'
        bad_line = contents_lines[bad_linenum - 1]        # convert to 0-index

        if '@Nolint' in bad_line:
            return 0

        # We allow lines to be arbitrarily long if they are urls,
        # since splitting urls at 80 columns can be annoying.
        if ('E501 line too long' in output_line and
            ('http://' in bad_line or 'https://' in bad_line)):
            return 0

        # OK, looks like it's a legitimate error.
        print output_line,    # output_line already includes the trailing \n
        return 1

    def process(self, f, contents_of_f):
        contents_lines = contents_of_f.splitlines(True)

        (num_candidate_errors, pep8_stdout) = _capture_stdout_of(
            pep8.Checker(f, lines=contents_lines).check_all)

        # Go through the output and remove the 'actually ok' lines.
        if num_candidate_errors == 0:
            return

        for output_line in pep8_stdout.readlines():
            self._num_errors += self._process_one_line(output_line,
                                                       contents_lines)

    def num_errors(self):
        """A count of all the errors we've seen (and emitted) so far."""
        return self._num_errors


class Pyflakes(object):
    """process() processes one file."""
    def __init__(self):
        self._num_errors = 0

    def _process_one_line(self, output_line, contents_lines):
        """If line is a 'error', print it and return 1.  Else return 0.

        pyflakes prints all errors to stdout.  But we want to ignore
        some 'errors' that are ok for us: code like
          try:
             import unittest2 as unittest
          except ImportError:
             import unittest
        To do this, we intercept stdin and remove these lines.

        Arguments:
           output_line: one line of the pyflakes error-output
           contents_lines: the contents of the file being linted,
              as a list of lines.

        Returns:
           1 (indicating one error) if we print the error line, 0 else.
        """
        # The 'try/except ImportError' example described above.
        if 'redefinition of unused' in output_line:
            return 0

        # We follow python convention of allowing an unused variable
        # if it's named '_' or starts with 'unused_'.
        if ('assigned to but never used' in output_line and
            ("local variable '_'" in output_line or
             "local variable 'unused_" in output_line)):
            return 0

        # Get rid of some warnings too.
        if 'unable to detect undefined names' in output_line:
            return 0

        # -- The next set of warnings need to look at the error line.
        bad_linenum = int(output_line.split(':', 2)[1])   # first line is '1'
        bad_line = contents_lines[bad_linenum - 1]        # convert to 0-index

        # If the line has a nolint directive, ignore it.
        if '@Nolint' in bad_line:
            return 0

        # An old nolint directive that's specific to imports
        if ('@UnusedImport' in bad_line and
            'imported but unused' in output_line):
            return 0

        # OK, looks like it's a legitimate error.
        print output_line,    # output_line already includes the trailing \n
        return 1

    def process(self, f, contents_of_f):
        # pyflakes's ast-parser fails if the file doesn't end in a newline,
        # so make sure it does.
        if not contents_of_f.endswith('\n'):
            contents_of_f += '\n'
        (num_candidate_errors, pyflakes_stdout) = _capture_stdout_of(
            pyflakes.check, contents_of_f, f)

        # Now go through the output and remove the 'actually ok' lines.
        if num_candidate_errors == 0:
            return

        contents_lines = contents_of_f.splitlines()  # need these for filtering
        for output_line in pyflakes_stdout.readlines():
            self._num_errors += self._process_one_line(output_line,
                                                       contents_lines)

    def num_errors(self):
        """A count of all the errors we've seen (and emitted) so far."""
        return self._num_errors


def main(rootdir, pep8_args, pyflakes_args):
    blacklist = _parse_blacklist(_BLACKLIST_FILE)
    files = _files_to_process(rootdir, blacklist)

    io_errors = 0
    processors = (Pep8(files, pep8_args),
                  Pyflakes()
                  )
    for f in files:
        try:
            contents = open(f, 'U').read()
        except (IOError, OSError), why:
            print "%s: %s" % (f, why.args[1])
            io_errors += 1
            continue
        for processor in processors:
            processor.process(f, contents)

    return io_errors + sum(p.num_errors() for p in processors)


if __name__ == '__main__':
    num_errors = main('.', [sys.argv[0]] + _DEFAULT_PEP8_ARGS, [])
    sys.exit(num_errors)
