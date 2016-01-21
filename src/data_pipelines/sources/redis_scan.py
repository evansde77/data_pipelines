#!/usr/bin/env python
"""
redis_scan

Data Source that uses a redis scan command to generate
a data source

"""
import redis
from data_pipelines.data_source import DataSource


class RedisScan(DataSource):
    """
    Simple data source that runs a redis SCAN operation
    with an optional match and count and iterates over the results

    """
    def __init__(self, **kwargs):
        self.host = kwargs.pop('host', 'localhost')
        self.port = kwargs.pop('port', 6379)
        self.db = kwargs.pop('db', 0)
        self.connect_args = kwargs.pop('connect_options', {})
        self.match = kwargs.pop('match', None)
        self.count = kwargs.pop('count', None)
        self._redis = None
        self._iter = None

    def connect(self):
        self._redis = redis.Redis(
            host=self.host,
            port=self.port,
            db=self.db,
            **self.connect_args
        )
        self._iter = self._redis.scan_iter(
            match=self.match,
            count=self.count
        )

    def disconnect(self):
        del self._redis
        self._redis = None
        self._iter = None

    def next(self):
        val = self._iter.next()
        return self._redis.get(val)
