"""Classes to extract useful data from App Engine admin console pages."""

from lxml import html
import re

# TODO(chris): implement more parsers.
#
# Instances:
# - expand detail dicts to contain all info (but no links).
#
# Logs:
# - application_id()/version_id()
# - logs_storage() -> {total: "", version: ""}
#
# Deployment:
# - application_id()
# - version_dicts()
#   {version_id: "", size: "", runtime: "", api_version: "", is_default: "",
#    deployed: ""}
#
# Backends:
# - application_id()
# - backend_dicts()
#   {version_id: "", size: "", runtime: "", api_version: "", deployed: "",
#    class: "B2", num_instances: "", options: ""}
#
# Queues:
# - application_id()
# - summary_dict()
#   {api_calls: "", stored_task_count: "", stored_task_bytes: ""}
# - push_queue_detail_dicts()
#   {queue_name: "", max_rate: "", enforced_rate: "", bucket_size: "",
#    oldest_task: "", in_queue: "", "run_last_minute": "", running: ""}
#
# BillingHistory:
# - event_dicts() for event types other than usage reports.
# - filtered_event_dicts(title="")


def text(html_element):
    """Returns the textual content within a node and its children.

    Whereas element.text returns only the contents of the first text
    node child of a node, this function aggregates all text nodes
    within itself and its children.

    Given the "p" element in the following markup, .text returns
    "There are " while this function returns "There are 5 pears".

    <p>There are <b>5</b> pears</p>

    """
    return html_element.xpath("string()")


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

    def raw_summary_dicts(self):
        """Performance statistics summarized across instances.

        Returns:
          A list of one or more dicts with fields like these:

          [{'appengine_release': '1.9.2',
            'total_instances': '100 total (10 Resident)',
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
                'appengine_release': children[0].text.strip(),
                'total_instances': children[1].text.strip().replace('\n', ' '),
                'average_qps': children[2].text.strip(),
                'average_latency': children[3].text.strip(),
                'average_memory': children[4].text.strip()
            })
        return summaries

    def summary_dict(self):
        """A parsed representation of performance statistics.

        Raises ValueError if unable to parse elements of the raw
        summary. For example if input like '180.3 ms' later changes to
        '0.1803 s' ValueError will be raised and this code will need an
        update.

        Returns:
          A dict with fields like this:

          {'total_instances': 100,
           'average_qps': 2.243,
           'average_latency_ms': 180.3,
           'average_memory_mb': 134.8}
        """
        summary_dicts = []
        raw_summary_dict = self.raw_summary_dicts()
        # Validate the raw summary and convert to a parsed
        # representation using this table of tuples whose fields are:
        #   (OUTPUT_FIELD, INPUT_FIELD, PATTERN, MATCHED_GROUP_PARSER)
        fields = (('total_instances', 'total_instances',
                   r'^(\d+) total.*', int),
                  ('average_qps', 'average_qps',
                   r'^(\d+(?:\.\d+)?$)', float),
                  ('average_latency_ms', 'average_latency',
                   r'^(\d+(?:\.\d+)?) ms$', float),
                  ('average_memory_mb', 'average_memory',
                   r'^(\d+(?:\.\d+)?) MBytes$', float),
                 )
        for raw_summary_dict in self.raw_summary_dicts():
            summary_dict = {}
            for (out_field, in_field, pattern, fn) in fields:
                match = re.match(pattern, raw_summary_dict[in_field])
                if not match:
                    raise ValueError('Summary field %s did not match '
                                     'pattern %s' % (in_field, pattern))
                summary_dict[out_field] = fn(match.group(1))
            summary_dicts.append(summary_dict)
        # Reduce to a single dict with weighted averages for each
        # field except "total_instances".
        total_instances = sum(d['total_instances'] for d in summary_dicts)
        summary = {'total_instances': total_instances}
        for field, _, _, _ in fields[1:]:
            instance_weighted_sum = sum(d['total_instances'] * d[field]
                                        for d in summary_dicts)
            summary[field] = float(instance_weighted_sum) / total_instances
        # Beautify rounding precision to match the App Engine UI.
        summary['average_qps'] = round(summary['average_qps'], 3)
        summary['average_latency_ms'] = round(summary['average_latency_ms'], 1)
        summary['average_memory_mb'] = round(summary['average_memory_mb'], 1)
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

    def statistics_dict(self):
        """A parsed representation of memcache statistics.

        Raises ValueError if unable to parse elements of the raw
        summary. For example if input like '1024 byte(s)' later changes
        to '1 kilobyte(s)' a ValueError will be raised.

        Returns:
          A dict with fields like this:

          {'hit_count': 12345,
           'miss_count': 123,
           'hit_ratio': 0.99,
           'item_count': 678,
           'total_cache_size': 91011,
           'oldest_item_age': 26}
        """
        raw_dict = self.raw_statistics_dict()
        parsed_dict = {}
        # Validate the raw summary and convert to a parsed
        # representation using this table of tuples whose fields are:
        #   (OUTPUT_FIELD, INPUT_FIELD, PATTERN, MATCHED_GROUP_PARSER)
        fields = (('hit_count', 'hit_count', r'^(\d+)$', int),
                  ('miss_count', 'miss_count', r'^(\d+)$', int),
                  ('hit_ratio', 'hit_ratio', r'^(\d+)%$', int),
                  ('item_count', 'item_count', r'^(\d+) item\(s\)$', int),
                  ('total_cache_size_bytes', 'total_cache_size',
                   r'^(\d+)$', int),
                  ('oldest_item_age_seconds', 'oldest_item_age',
                   r'^(?:(\d+) day\(s\)\s+)?'
                   r'(?:(\d+) hour\(s\)\s+)?'
                   r'(?:(\d+) min\(s\)\s+)?'
                   r'(\d+) second\(s\)$',
                   int),
                 )
        for (out_field, in_field, pattern, fn) in fields:
            match = re.match(pattern, raw_dict[in_field])
            if not match:
                raise ValueError('Field "%s" did not match %r on %r' %
                                 (in_field, pattern, raw_dict[in_field]))
            if in_field == 'oldest_item_age':
                seconds = int(match.group(4))
                if match.group(3):
                    seconds += int(match.group(3)) * 60  # minutes
                if match.group(2):
                    seconds += int(match.group(2)) * 60 * 60  # hours
                if match.group(1):
                    seconds += int(match.group(1)) * 60 * 60 * 24  # days
                value = seconds
            else:
                value = match.group(1)
            parsed_dict[out_field] = fn(value)
        parsed_dict['hit_ratio'] = float(parsed_dict['hit_ratio']) / 100
        return parsed_dict

    def raw_statistics_dict(self):
        """Memcache statistics for the current application.

        Returns:
          A dict with fields like this:

          {'hit_count': '12345',
           'miss_count': '123',
           'hit_ratio': '99%',
           'item_count': '678 item(s)',
           'total_cache_size': '91011',
           'oldest_item_age': '19 day(s) 20 hour(s) 27 min(s) 26 second(s)'}
        """
        stats = {}
        fields = {'Hit count:': 'hit_count',
                  'Miss count:': 'miss_count',
                  'Hit ratio:': 'hit_ratio',
                  'Item count:': 'item_count',
                  'Total cache size:': 'total_cache_size',
                  'Oldest item age:': 'oldest_item_age'}
        selector = '#ae-stats-table tr'
        for element in self.doc.cssselect(selector):
            children = list(element)
            assert len(children) == 2, [text(child) for child in children]
            if text(children[0]) and text(children[0]).strip() in fields:
                # skip rows with invalid or empty cells
                field_name = fields[text(children[0]).strip()]
                stats[field_name] = text(children[1]).strip()
        # Ensure all fields were filled.
        assert len(stats) == len(fields), (fields.keys(), stats.keys())
        return stats
