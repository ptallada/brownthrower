#! /usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import random
import logging

import base
import brownthrower as bt

from sqlalchemy.orm import scoped_session
from sqlalchemy.orm.session import sessionmaker

from testconfig import config # @UnresolvedImport

log = logging.getLogger('nose')

def setup():
    # Retrieve database settings. Use in-memory sqlite by default.
    url  = config.get('database', {}).get('url', 'sqlite:///')
    
    base.BaseTest._session_maker = bt.session_maker(url)
    
    logging.basicConfig(level = logging.DEBUG)
    logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
    
    if config.get('pydevd', {}).get('enabled', 'off') == 'on':
        import pydevd
        pydevd.settrace()

def teardown():
    #base.BaseTest._session_maker.metadata.drop_all()
    pass
