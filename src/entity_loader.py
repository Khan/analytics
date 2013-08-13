"""Loads entities from the daily backups.

The following entity types are supported:
    Exercise
    Feedback
    LearningTask
    Scratchpad
    StackLog
    Topic
    UserBadge
    UserEvent
    UserVideo
    VideoLog
    GAEBingoIdentityRecord
    ProblemLog
    ScratchpadRevision
    UserAssessment
    UserData
    Video

The following entity types are not supported:
    ExerciseVideo
    UserMission

Encodings:
    json is supported
    pickle isn't

Usage:
    loader = EntityLoader()
    for log in loader.entities("VideoLog"):
        data = log.get('json')
        # Do stuff with video logs

    for log in loader.entities("ProblemLog"):
        data = log.get('json')
        # Do stuff with problem logs
"""

import datetime
import json
import gzip
import os


class EntityLoader(object):

    # format string for entity directories
    # The date is isoformat, so we can just call str(date) for %(date)s
    dir_fmt = "%(prefix)s/%(date)s/%(type)s"

    schemas = {
        # From ka_hive_init.q
        'Exercise':               ['key', 'json'],
        'Scratchpad':             ['key', 'json'],
        'StackLog':               ['user', 'json'],
        'Topic':                  ['key', 'json'],
        'UserBadge':              ['user', 'json'],
        'UserEvent':              ['user', 'json'],
        'VideoLog':               ['user', 'json'],
        'GAEBingoIdentityRecord': ['key', 'json'],
        'ProblemLog':             ['user', 'json'],
        'ScratchpadRevision':     ['key', 'json'],
        'UserAssessment':         ['key', 'json'],
        'UserData':               ['key', 'json'],
        'Video':                  ['key', 'json'],

        # By inspection, not in ka_hive_init.q
        'Feedback':               ['key', 'json'],
        'LearningTask':           ['key', 'json'],
        'UserVideo':              ['user', 'json'],
    }

    def __init__(self,
                 data_prefix="/ebs/kadata2/daily_new",
                 encoding="json",
                 schemas={}):
        self.data_prefix = data_prefix
        self.encoding = encoding
        self.schemas.update(schemas)

    def today(self):
        return datetime.date.today()

    def dates(self, end_date=None, begin_date=None):
        """Generator yields dates in reverse chronological order,
        from end_date to begin_date

        Starts at the end_date (today by default) and goes back one day at
        a time until begin_date is reached. Goes forever if begin_date is None.
        """

        if end_date is None:
            end_date = self.today()

        if begin_date is not None and end_date < begin_date:
            begin_date, end_date = end_date, begin_date

        current_date = end_date
        while begin_date is None or current_date >= begin_date:
            yield current_date

            # Go back one day
            current_date -= datetime.timedelta(days=1)

    def entity_dirname(self, type, date=None):
        if date is None:
            date = self.today()

        return self.dir_fmt % {
            "prefix": self.data_prefix,
            "date": str(date),
            "type": type
        }

    def entity_dirnames(self, type, end_date=None, begin_date=None):
        for date in self.dates(end_date=end_date, begin_date=begin_date):
            yield self.entity_dirname(type, date)

    def entity_filenames(self, type, end_date=None, begin_date=None):
        dirnames = self.entity_dirnames(
            type=type,
            end_date=end_date,
            begin_date=begin_date)
        missing_dirs = 0
        for dirname in dirnames:
            if not os.path.exists(dirname):
                missing_dirs += 1
                if missing_dirs > 5:
                    # We've probably run out of data
                    raise StopIteration
                continue

            for filename in reversed(sorted(os.listdir(dirname))):
                if self.encoding in filename:
                    yield "%(path)s/%(filename)s" % {
                        "path": dirname,
                        "filename": filename
                    }

    def entity_files(self, type, end_date=None, begin_date=None):
        filenames = self.entity_filenames(
            type=type,
            end_date=end_date,
            begin_date=begin_date)
        for filename in filenames:
            entity_file = gzip.open(filename, 'rb')
            yield entity_file
            entity_file.close()

    def entities_in_file(self, type, entity_file, filters=None):
        if type not in self.schemas:
            raise ValueError('Invalid type %s' % type)
        schema = self.schemas[type]

        for entity_string in entity_file:
            if self.encoding == 'json':
                data = entity_string.split("\t")
                entity = {}

                for i in xrange(len(schema)):
                    key = schema[i]
                    value = data[i]
                    if key == 'json':
                        value = json.loads(value)
                    entity[key] = value

                # TODO(Bieber): Handle filters
                yield entity

            elif self.encoding == 'pickle':
                # TODO(Bieber): Support pickle
                raise ValueError('pickle not supported')
            else:
                raise ValueError('Invalid encoding %s' % self.encoding)

    def entities(self,
                 type,
                 end_date=None,
                 begin_date=None,
                 limit=1000,
                 filters=None):
        count = 0
        entity_files = self.entity_files(
            type=type,
            end_date=end_date,
            begin_date=begin_date)
        for entity_file in entity_files:
            ents = self.entities_in_file(type, entity_file, filters=filters)
            for entity in ents:
                if limit is not None and count >= limit:
                    raise StopIteration

                count += 1
                yield entity
