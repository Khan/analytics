#!/usr/bin/env python

import datetime
import unittest

import date_util


class TestDateUtil(unittest.TestCase):
    def test_get_week_boundaries(self):
        sunday = datetime.date(2012, 6, 17)

        for i in range(0, 7):
            test_date = sunday + datetime.timedelta(days=i)
            week = date_util.get_week_boundaries(test_date)
            self.assertEquals(sunday, week[0])
            self.assertEquals(week[1] - week[0], datetime.timedelta(days=7))


if __name__ == '__main__':
    unittest.main()

