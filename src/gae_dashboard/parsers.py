"""Classes to extract useful data from App Engine admin console pages."""

# Has a "debug mode" when __name__ == '__main__'. See the bottom of
# the file for details.

from lxml import html
import re
import sys


def text(html_element):
    """Returns the textual content within a node and its children.

    Whereas element.text returns only the contents of the first text
    node child of a node, this function aggregates all text nodes
    within itself and its children.

    Given the "p" element in the following markup, .text returns
    "There are " while this function returns "There are 5 pears".

    <p>There are <b>5</b> pears</p>

    """
    return html_element.xpath("string()") or ''  # avoid returning None


def _to_num(val):
    """Return first numeric thing as float or int.

    Some examples:

      "1,234" -> 1234

      "There were 5 flies." -> 5

      "1,234.56" -> 1234.56

    """
    m = re.search(r'\d+(?:\.\d+)?', val.replace(',', ''))
    if not m:
        raise ValueError('No number found in %s' % val)
    numstr = m.group(0)
    if '.' in numstr:
        return float(numstr)
    else:
        return int(numstr)


class Value(object):
    """Information about a scraped value.

    A scraped value always has two components: the scraped HTML text,
    like "1,234 instances" and the parsed value, like the integer 1234.

    This object retains both parts of the original value and provides
    class methods that make it easier to parse out the value portion
    from scraped HTML text.
    """
    def __init__(self, text, value):
        self._text = self._normalize_html(text)
        self._value = value

    def text(self):
        """Scraped HTML text."""
        return self._text

    def value(self):
        """Value derived from the scraped HTML text."""
        return self._value

    def __str__(self):
        return str(self.value())

    @classmethod
    def _normalize_html(cls, s):
        # Strip leading/trailing whitespace and normalize internal
        # whitespace to how HTML displays text.
        return ' '.join(s.split()).strip()

    @classmethod
    def from_number(cls, value_str):
        """Read a number: "1,234.56" -> float(1234.56) and "12" -> int(12)."""
        return cls(value_str, _to_num(value_str))

    @classmethod
    def from_percent(cls, value_str):
        """Read "93%" into the float value 0.93."""
        return cls(value_str, _to_num(value_str) / 100.0)

    @classmethod
    def from_str(cls, value_str):
        """Read "  One fish\nTwo fish " as the string "One fish Two fish"."""
        return cls(value_str, cls._normalize_html(value_str))

    @classmethod
    def from_time_ago(cls, value_str):
        """Read "3 day(s) 23 hour(s) 38 min(s) 16 second(s)" as 344296."""
        value_str = cls._normalize_html(value_str)
        value_str = value_str + ' '  # add trailing space to simplify regex
        # Only one of these time specifiers must be found.
        pattern = (r'^(?:(\d+) day\(s\)\s+)?'
                   r'(?:(\d+) hour\(s\)\s+)?'
                   r'(?:(\d+) min\(s\)\s+)?'
                   r'(?:(\d+) second\(s\)\s+)?$')
        match = re.match(pattern, value_str)
        if not match or not any(match.groups()):
            raise ValueError('%r did not match in %r' % (pattern, value_str))
        seconds = 0
        if match.group(4):
            seconds += int(match.group(4))  # seconds
        if match.group(3):
            seconds += int(match.group(3)) * 60  # minutes
        if match.group(2):
            seconds += int(match.group(2)) * 60 * 60  # hours
        if match.group(1):
            seconds += int(match.group(1)) * 60 * 60 * 24  # days
        return cls(value_str, seconds)


class BaseParser(object):
    """A shared base class for common operations."""

    def __init__(self, html_contents):
        """Initialize with a string containing the dashboard page's HTML."""
        self.doc = html.document_fromstring(html_contents)

    def application_id(self):
        selector = '#ae-appbar-app-id option[selected="selected"]'
        # There should be exactly one selected application id.
        (app_id_element, ) = self.doc.cssselect(selector)
        value = app_id_element['value']
        assert value.startswith('s~'), value
        return value[2:]


class BillingHistory(BaseParser):
    """An API for the contents of /billing/history as structured data."""

    def _usage_report_dict(self, root):
        """Extract usage report details from the element that contains
        the table with columns resource, unit, used."""
        details = {}
        selector = 'table > tbody > tr'
        for (resource, unit, used) in root.cssselect(selector):
            name = resource.findtext('strong').strip()
            details[name] = (used.text.strip(), unit.text.strip())
        return details

    def event_dicts(self):
        """Information about each row in the billing history table.

        Entries match the found in the HTML and have "date" and
        "title" fields. If the event is a usage report, the key
        "details" contains usage data. For example:

        {date: "2012-10-13 15:36:05",
         title: "Usage Report for 2012-10-13",
         details: {"Frontend Instance Hours": ('<X,XXX.XX>', 'Hour'),
                   "Backend Instance Hours": ('<X,XXX.XX>', 'Hour'),
                   ...}}
        """
        events = []
        # We're assuming that the table has alternating rows that
        # containg (date, event title) possibly followed by (<empty>,
        # event details).
        selector = '#ae-billing-logs-table > tbody > tr'
        for (date_elt, event_elt) in self.doc.cssselect(selector):
            if date_elt.text is not None:
                events.append({
                    # <td>EVENT DATE</td>
                    'date': date_elt.text.strip(),
                    # <td><span id="...">EVENT TITLE</span></td>
                    'title': event_elt.findtext('span').strip()
                })
            else:
                # An empty first column indicates details for the
                # preceeding event.
                assert len(events) > 0, len(events)
                last_event = events[-1]
                if last_event['title'].startswith('Usage Report '):
                    last_event['details'] = self._usage_report_dict(event_elt)
        return events


class Dashboard(BaseParser):
    """An API for the contents of /dashboard as structured data."""

    def charts(self, time_span=None):
        """The labels and URLs of dashboard charts.

        These are the charts at the top of the dashboard page,
        selected by the dropdown menu.

        Arguments:
          time_span (optional): return only charts matching this time
            span. Possible values are "6 hrs", "12 hrs", "24 hrs". By
            default, all charts are returned.

        Returns:
          (label, url) pairs. For example, ('Requests/Second', ...).
          When returning all charts, the time span is included in the
          chart label, e.g., ('Requests/Second (6 hrs)', ...).

        NOTE: Other time spans are not supported yet and those chart
        URLs are not in the HTML of /dashboard on initial page load.
        """
        assert time_span in (None, '6 hrs', '12 hrs', '24 hrs'), time_span
        selector = '#chart option'
        for element in self.doc.cssselect(selector):
            label = element.text.strip()
            chart_id = element.attrib['value']
            hidden_input = self.doc.get_element_by_id('ae-dash-graph-' +
                                                      chart_id)
            url = hidden_input.attrib['value']
            if not url:
                continue
            if time_span is None:
                yield label, url
            elif label.endswith(' (%s)' % time_span):
                yield label.replace(' (%s)' % time_span, ''), url


class InstanceSummary(BaseParser):
    """An API for the contents of /instance_summary as structured data."""

    def summaries(self):
        """Performance statistics summarized by App Engine release.

        Returns:
          A list of one or more dicts with fields like these, where
          value is a Value instance whose .text() is shown as an
          example:

          [{'appengine_release': '1.9.2',
            'total_instances': '100 total',
            'average_qps': '2.243',
            'average_latency': '180.3 ms',
            'average_memory': '134.8 MBytes'},
           ...]

        """
        summaries = []
        selector = '#ae-content tr'
        # The table has multiple rows when a new version is rolling out.
        rows = self.doc.cssselect(selector)
        assert len(rows)
        for row in rows:
            children = list(row)
            # Expecting 'App Engine Release', 'Total number of instances',
            # 'Average QPS', 'Average Latency', 'Average Memory'
            assert len(children) == 5, [child.text for child in children]
            summaries.append({
                'appengine_release': Value.from_str(text(children[0])),
                'total_instances': Value.from_number(text(children[1])),
                'average_qps': Value.from_number(text(children[2])),
                'average_latency': Value.from_number(text(children[3])),
                'average_memory': Value.from_number(text(children[4])),
            })
        return summaries

    def summary(self):
        """Performance statistics summarized across all instances.

        Returns:
          A dict with fields like these:

          {'total_instances': 100,
           'average_qps': 2.243,
           'average_latency_ms': 180.3,
           'average_memory_mb': 134.8}

        """
        summaries = self.summaries()
        # Reduce to a single summary with weighted averages for each
        # field except "total_instances", which is summed.
        total_instances = sum(d['total_instances'].value() for d in summaries)
        summary = {'total_instances': total_instances}
        for field in ('average_qps', 'average_latency', 'average_memory'):
            instance_weighted_sum = sum(
                d['total_instances'].value() * d[field].value()
                for d in summaries)
            summary[field] = float(instance_weighted_sum) / total_instances
        # Beautify rounding precision to match the App Engine UI.
        summary['average_qps'] = round(summary['average_qps'], 3)
        summary['average_latency_ms'] = round(summary['average_latency'], 1)
        summary['average_memory_mb'] = round(summary['average_memory'], 1)
        del summary['average_latency']
        del summary['average_memory']
        return summary


class Instances(BaseParser):
    """An API for the contents of /instances as structured data."""

    def version(self):
        """The app version that owns these instances."""
        selector = '#ae-appbar-version-id option[selected="selected"]'
        # There should be exactly one selected version.
        (version_element, ) = self.doc.cssselect(selector)
        return version_element.text.strip()

    def raw_detail_dicts(self):
        """Performance statistics specific to each instance.

        Returns:
          A list of dicts with (as of App Engine 1.7.2) fields like this:

          [{'instance_id': '01c61b117c08b2b562c94f26f43f9b04f6775180',
            'qps': '1.183',
            'latency': '208.5 ms',
            'requests': 14628,
            'errors': 5,
            'age': '9:07:52',
            'memory': '184.8 MBytes'},
           ...
          ]
        """
        # TODO(chris): validate columns using column headers.
        details = []
        selector = '#ae-instances-details-table tbody tr'
        for element in self.doc.cssselect(selector):
            children = list(element)
            assert len(children) == 9, [child.text for child in children]
            details.append({
                'instance_id': element.attrib['id'].strip(),
                'qps': children[0].text.strip(),
                'latency': children[1].text.strip(),
                'requests': children[2].text.strip(),
                'errors': children[3].text.strip(),
                'age': children[4].text.strip(),
                'memory': children[5].text.strip()
            })
        return details


class Memcache(BaseParser):
    """An API for the contents of /memcache as structured data."""
    def statistics(self):
        """Memcache statistics for the current application.

        Returns:
          A dict with fields like this, where value is a Value
          instance whose .text() is shown as an example:

          {'hit_count': '12345',
           'miss_count': '123',
           'hit_ratio': '99%',
           'item_count': '678 item(s)',
           'total_cache_size': '91011',
           'oldest_item_age': '19 day(s) 20 hour(s) 27 min(s) 26 second(s)'}

        """
        stats = {}
        fields = {
            'Hit count:': ('hit_count', Value.from_number),
            'Miss count:': ('miss_count', Value.from_number),
            'Hit ratio:': ('hit_ratio', Value.from_percent),
            'Item count:': ('item_count', Value.from_number),
            'Total cache size:': ('total_cache_size', Value.from_number),
            'Oldest item age:': ('oldest_item_age', Value.from_time_ago),
            }
        selector = '#ae-stats-table tr'
        for element in self.doc.cssselect(selector):
            children = list(element)
            assert len(children) == 2, [text(child) for child in children]
            if text(children[0]).strip() in fields:
                # skip rows with invalid or empty cells
                field_name, value_fn = fields[text(children[0]).strip()]
                stats[field_name] = value_fn(text(children[1]))
        # Ensure all fields were filled.
        assert len(stats) == len(fields), (fields.keys(), stats.keys())
        return stats


class Deployment(BaseParser):
    """An API for the contents of /deployment as structured data."""

    def default_version(self):
        """Default version as a string, e.g., "1"."""
        # Seen in 1.9.3 within the version table and only for the
        # default version:
        #
        # <strong class="ae-deployment-live" id="ae-deployment-live<VERSION>">
        # Yes
        # </strong>
        #
        cssclass = 'ae-deployment-live'
        elements = list(self.doc.cssselect('.' + cssclass))
        assert len(elements) == 1, elements
        child = elements[0]
        assert text(child).strip() == 'Yes', text(child)
        assert child.attrib['id'].startswith(cssclass), child.attrib['id']
        return Value.from_str(child.attrib['id'][len(cssclass):])


if __name__ == '__main__':
    # Entry point for debugging. Create a parser that wraps the
    # contents of stdin and call the first argument on it as a
    # function and print the result, e.g.,
    #
    #   python parsers.py Dashboard summary_dict <dashboard.html
    #
    # Is like:
    #
    #   print Dashboard(open('dashboard.html').read()).summary_dict()
    #

    # Some late imports only needed for debugging output.
    import pprint
    import types
    _, parser_class, method = sys.argv
    parser = globals()[parser_class](sys.stdin.read())
    value = getattr(parser, method)()
    if isinstance(value, types.GeneratorType):
        value = list(value)  # unpack generator
    pprint.pprint(value)
