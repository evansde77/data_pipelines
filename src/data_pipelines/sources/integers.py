#!/usr/bin/env python
"""
integers

generate a list of integers for testing

"""

from data_pipelines.data_source import DataSource


class Integers(DataSource):
    """
    Simple data source that runs a redis SCAN operation
    with an optional match and count and iterates over the results

    """
    def __init__(self, **kwargs):
        super(DataSource, self).__init__()
        self.limit = kwargs.pop('limit', 1000)
        self.skip = kwargs.pop('skip', 0)

    def connect(self):
        self._iter = (x for x in range(self.skip, self.limit))

    def disconnect(self):
        self._iter = None

    def next(self):
        return self._iter.next()
