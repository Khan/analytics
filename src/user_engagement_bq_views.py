"""Script for generating views in BigQuery for user engagement stats.

The pain points for generating the views:

- We want the column names themselves to be reasonably short (describing the
  segment), using aliases.
- Cyfe defaults to having a separate y-axis for each value (for line graphs),
  while in most cases we want to compare along the same y-axis. The workaround
  is to generate a row "YAxis,0,0,...0" at the end. We can do this in BigQuery
  with a union equivalent, i.e. comma-separating tables with matching column
  names. See: http://www.cyfe.com/custom
- The segments and/or metrics may grow.

TODO(tony): programmatically update the views as well!
"""

# Mapping from key to a list of column prefixes
SEGMENTS = {
    'product': [
        'dashboard', 'tutorial_mode', 'both',
    ],
    'coach_status': [
        'uncoached', 'coached_by_teacher', 'coached_other',
    ],
    'active_days': [
        'active_%d_days' % d for d in xrange(1, 8)
    ],
}

# A list of metrics we'd like to track for each segment
METRICS = [
    'users',
    'daily_problem_count',
    'daily_problem_seconds',
    'daily_video_seconds',
]


def generate_query_for_segment(segment, metric):
    selectors = ['week_date']

    for prefix in SEGMENTS[segment]:
        selectors.append('%(prefix)s_%(metric)s as %(prefix)s' % {
                'prefix': prefix,
                'metric': metric,
            })

    # Cyfe-specific row: we generate this in the api_public view and not the
    # data itself as to not pollute the user_engagement table
    if metric != 'users':
        appended_rows = """, ( SELECT 'YAxis' week_date,
                %(selectors)s )
        """ % {
            'selectors': ',\n'.join([
                '0.0 %s_%s' % (prefix, metric) for prefix in SEGMENTS[segment]
            ]),
        }
        appended_rows += """, ( SELECT 'Cumulative' week_date,
                %(selectors)s )
        """ % {
            'selectors': ',\n'.join([
                '0.0 %s_%s' % (prefix, metric) for prefix in SEGMENTS[segment]
            ]),
        }
    else:
        appended_rows = ''

    # Final query
    query = """
        SELECT %(selectors)s
        FROM latest_derived.user_engagement %(appended_rows)s
        ORDER BY week_date
    """ % {
        'selectors': ',\n'.join(selectors),
        'appended_rows': appended_rows,
    }
    return query


def main():
    # We can just copy/paste these
    for m in METRICS:
        for s in SEGMENTS:
            print '==='
            print 'view name: ', 'user_engagement_%s_%s' % (s, m)
            print 'query:'
            print generate_query_for_segment(s, m)


if __name__ == '__main__':
    main()
