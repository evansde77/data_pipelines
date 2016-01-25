#!/usr/bin/env python
"""
server

uwsgi spooler driven server for executing pipelines in response
to REST calls

"""

import sys
import logging

LOGGER = logging.getLogger('data_pipelines')
HANDLER = None


def get_logger():
    """set up a basic logger"""
    global HANDLER
    if HANDLER is None:
        LOGGER.setLevel(logging.DEBUG)

        HANDLER = logging.StreamHandler(sys.stdout)
        HANDLER.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        HANDLER.setFormatter(formatter)
        LOGGER.addHandler(HANDLER)
    return LOGGER
