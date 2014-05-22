"""Utility functions for sending data to graphite.

Graphite is our timeseries-graphing tool.  We send statistics from
the GAE admin dashboard to graphite in order to graph them.
"""

import cPickle
import datetime
import os
import socket
import struct


def maybe_send_to_graphite(graphite_host, category, records):
    """Send dashboard statistics to the graphite timeseries-graphing tool.

    This requires /home/analytics/hostedgraphite_secret exist and
    hold the hostedgraphite API key.  See aws-config/analytics/setup.sh.

    Arguments:
        graphite_host: hostname:port (port should be the port for the
            pickle protocol, probably 2004), or '' or None to avoid
            sending data to graphite.
        category: a string to identify the source of this data.
            The key we send to graphite will be
                 webapp.gae.dashboard.<category>.<statistic>
        records: a list of dicts, where the key is a string and the
            value a number.  We send each record to graphite.  Each
            record *must* have a 'utc_datetime' field with a
            datetime.datetime() object that says when this record's
            data is from.
    """
    if not graphite_host:
        return

    # Load the api key that we need to send data to graphite.
    # This will (properly) raise an exception if this file isn't installed
    # (based on the contents of webapp secrets.py).
    with open(os.path.join(os.environ['HOME'], 'hostedgraphite_secret')) as f:
        api_key = f.read().strip()

    epoch = datetime.datetime.utcfromtimestamp(0)

    # The format of the pickle-protocol data is described at:
    # http://graphite.readthedocs.org/en/latest/feeding-carbon.html#the-pickle-protocol
    graphite_data = []
    for record in records:
        record = record.copy()    # since we're munging it in place

        timestamp = record.pop('utc_datetime')
        # Convert the timestamp to a time_t.
        timestamp = int((timestamp - epoch).total_seconds())

        for (field, value) in record.iteritems():
            key = '%s.webapp.gae.dashboard.%s.%s' % (api_key, category, field)
            graphite_data.append((key, (timestamp, value)))

    if graphite_data:
        (hostname, port_string) = graphite_host.split(':')
        host_ip = socket.gethostbyname(hostname)
        port = int(port_string)

        pickled_data = cPickle.dumps(graphite_data, cPickle.HIGHEST_PROTOCOL)
        payload = struct.pack("!L", len(pickled_data)) + pickled_data

        graphite_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        graphite_socket.connect((host_ip, port))
        graphite_socket.send(payload)
        graphite_socket.close()
