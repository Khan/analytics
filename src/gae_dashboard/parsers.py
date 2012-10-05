from lxml import cssselect
from lxml import html


class Instances(object):
    """An API for the contents of /instances as structured data."""

    def __init__(self, html_contents):
        """Constructor.

        Arguments:
          html_contents - The HTML contents of the /instances dashboard page as
            a string.
        """
        self.doc = html.document_fromstring(html_contents)

    def select_css(self, selector):
        select = cssselect.CSSSelector(selector)
        for match in select(self.doc):
            yield match

    def version(self):
        """The app version that owns these instances."""
        selector = '#ae-appbar-version-id option[selected="selected"]'
        # There shold be exactly one selected version.
        (version_element, ) = self.select_css(selector)
        return version_element.text.strip()

    def summary_dict(self):
        """Performance statistics summarized across instances.

        Returns:
          A dict with (as of App Engine 1.7.2) fields like this:

          {'total_instances': '100 total (10 Resident)',
           'average_qps': '2.243',
           'average_latency': '180.3 ms',
           'average_memory': '134.8 MBytes'}
        """
        selector = '#ae-instances-summary-table tbody tr'
        # The table should have exactly one row.
        (row, ) = self.select_css(selector)
        children = list(row)
        # Expecting 'Total number of instances', 'Average QPS',
        # 'Average Latency', 'Average Memory'
        assert len(children) == 4
        return {
            'total_instances': children[0].text.strip().replace('\n', ' '),
            'average_qps': children[1].text.strip(),
            'average_latency': children[2].text.strip(),
            'average_memory': children[3].text.strip()
        }

    def detail_dicts(self):
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
        for element in self.select_css(selector):
            children = list(element)
            assert len(children) == 9
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
