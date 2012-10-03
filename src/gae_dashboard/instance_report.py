#!/usr/bin/env python

"""Scrape the GAE /instances dashboard into JSON.

Usage: instance_report.py <instances.html
"""

import json
import sys

from lxml import html
from lxml import cssselect


def main():
    doc = html.parse(sys.stdin)

    instances = []

    # TODO(chris): validate columns using column headers.

    select = cssselect.CSSSelector('#ae-instances-details-table tbody tr')
    for element in select(doc):
        children = list(element)
        assert len(children) == 9
        instances.append({
            'instance_id': element.attrib['id'].strip(),
            'qps': children[0].text.strip(),
            'latency': children[1].text.strip(),
            'requests': children[2].text.strip(),
            'errors': children[3].text.strip(),
            'age': children[4].text.strip(),
            'memory': children[5].text.strip()
        })

    print json.dumps(instances, sort_keys=True, indent=4)

if __name__ == '__main__':
    main()
