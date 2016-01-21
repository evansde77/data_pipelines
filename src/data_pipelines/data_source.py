#!/usr/bin/env python
"""
data_source

Base class for Data Source plugins

"""

from pluggage.factory_plugin import PluggagePlugin


class DataSource(PluggagePlugin):
    """
    Data Source
    """
    PLUGGAGE_FACTORY_NAME = 'data_pipelines.sources'

    def __init__(self):
        super(DataSource, self).__init__()

    def connect(self):
        pass

    def disconnect(self):
        pass

    def __iter__(self):
        return self

    def next(self):
        raise StopIteration
