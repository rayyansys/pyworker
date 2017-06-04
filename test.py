#!/usr/bin/env python

from os import environ as env
import logging
from pyworker.worker import Worker

dbstring = env.get('DATABASE_URL')
if not dbstring:
    raise EnvironmentError('DATABASE_URL missing from environment')

logging.basicConfig()
logger = logging.getLogger('pyworker')
logger.setLevel(logging.DEBUG)

w = Worker(dbstring, logger) # logger is optional
w.run()
