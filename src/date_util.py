"""Date utilities for common datetime conventions/patterns in analytics."""

import datetime
import string


def from_date_iso(s_date):
    """Parse a string in our modified ISO8601 format (no microseconds)."""
    return datetime.datetime.strptime(s_date, "%Y-%m-%dT%H:%M:%SZ")


def to_date_iso(date, micro=False):
    """
    Converts a datetime object to our modified ISO 8601 format
    (optionally keeping the microseconds).
    """
    datestring = date.isoformat()
    if not micro:
        idx = string.rfind(datestring, '.')
        if idx != -1:
            datestring = datestring[:idx]
        return "%sZ" % datestring
    return datestring


def get_week_boundaries(date):
    """Returns a tuple specifying the week boundaries for the given date.

    Specifically, this returns a tuple of the two adjacent Sundays that
    contain the specified date.
    """

    # weekday() is 0-based started on Monday - gotta offset to Sunday.
    days_into_week = (date.weekday() + 1) % 7

    start = date - datetime.timedelta(days=days_into_week)
    end = start + datetime.timedelta(days=7)
    return (start, end)


