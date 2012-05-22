"""Date utilities for common datetime conventions/patterns in analytics."""

import datetime
import string


def from_date_iso(s_date):
    """Parse a string assumed to be in our ISO8601 format (no microseconds)."""
    return datetime.datetime.strptime(s_date, "%Y-%m-%dT%H:%M:%SZ")


def to_date_iso(date):
    """Converts a datetime object to our ISO 8601 format (no microseconds)."""
    datestring = date.isoformat()
    idx = string.rfind(datestring, '.')
    if idx != -1:
        datestring = datestring[:idx]
    return "%sZ" % datestring


