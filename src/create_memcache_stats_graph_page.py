#!/usr/bin/python

"""Reads *.appengine_stats.log files and emits an html page for viewing them.

The appengine_stats files have lines like:
1338970203 up h:108806878 m:20427990 bh:6429713404634 i:8389 b:56035223 oia:97

We parse the h: (hits), m: (misses), and first field (time_t) in order
to emit a graph showing time vs hit-rate.  There's lots of other stuff we
could display as well, but we don't.  Fields are described here:
   https://developers.google.com/appengine/docs/python/memcache/functions
under 'get_stats'.
"""

import datetime
import re
import sys


# The %s here is the actual data to be graphed (one per line).
# NOTE: an alternatite to google's jsapi is HighCharts
# (http://www.highcharts.com/products/highstock).  It can do a
# similar graph but without requiring flash.
_HTML = """\
<html>
  <head>
    <script type='text/javascript' src='http://www.google.com/jsapi'></script>
    <script type='text/javascript'>
      google.load('visualization', '1', {'packages':['annotatedtimeline']});
      google.setOnLoadCallback(drawChart);
      function drawChart() {
        var data = new google.visualization.DataTable();
        data.addColumn('date', 'Date');
        data.addColumn('number', 'hit-rate (percent)');
        data.addRows([
%s
        ]);

        var chart = new google.visualization.AnnotatedTimeLine(
          document.getElementById('chart_div'));
        chart.draw(data, {displayAnnotations: true});
      }
    </script>
  </head>

  <body>
    <hr>
    <div id='chart_div' style='width: 700px; height: 740px;'></div>
    <hr>
  </body>
</html>
"""


_LINE_RE = re.compile('^(\d+) (\S+)( ([^:]+):(\S+))*$')


def _ParseLine(line):
    """For a line like 'a:b c:d' return a map a=>b, c=>d.  Handles time_t."""
    # Input is '<time_t> <status> <field1>:<value1> <field2>:<value2> ...'
    fields = line.strip().split()
    retval = {'time_t': fields[0],
              'status': fields[1],
              }
    retval.update(dict(f.split(':') for f in fields[2:]))
    return retval


def _OneGraphRow(lastline_dict, line_dict):
    """Return a string suitable for putting in the output."""
    # Output is like '  [new Date(2012, 4, 22, 21, 30, 3), 89.15],'
    now = datetime.datetime.fromtimestamp(int(line_dict['time_t']))
    now_tuple = list(now.timetuple())
    # The graph library indexes months starting from 0, so we need to correct.
    now_tuple[1] -= 1
    # And we only want (Y, M, D, h, m, s), not day-of-week, timezone, etc.
    now_tuple = now_tuple[:6]

    delta_hits = int(lastline_dict['h']) - int(line_dict['h'])
    delta_misses = int(lastline_dict['m']) - int(line_dict['m'])
    hit_rate_pct = (delta_hits * 100.0) / (delta_hits + delta_misses)

    # We take advantage of the coincidence that python prints tuples in
    # just the format we need them here (so they look like fn args).
    return '  [new Date%s, %.2f],' % (tuple(now_tuple), hit_rate_pct)


def Html(f):
    """Reads lines from the given file object, then emits the html."""
    rows = []
    lastline_dict = None
    lines_printed = 0
    for line in f:
        try:
            line_dict = _ParseLine(line)
            if lastline_dict:
                rows.append(_OneGraphRow(lastline_dict, line_dict))
                lines_printed += 1
            lastline_dict = line_dict
        except (IndexError, ValueError, KeyError), why:
            print >>sys.stderr, 'Skipping line "%s": %s' % (line.strip(), why)
    print >>sys.stderr, 'Emitted %s lines.' % lines_printed
    return _HTML % '\n'.join(rows)


def main():
    print Html(sys.stdin)


if __name__ == '__main__':
    if len(sys.argv) > 1:
        sys.exit('USAGE: %s < <*.appengine_stats.log>' % sys.argv[0])
    main()
