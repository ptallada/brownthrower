#! /usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import random
import logging

import base
import brownthrower

from testconfig import config # @UnresolvedImport

log = logging.getLogger('nose')

def setup():
    # Retrieve database settings. Use in-memory sqlite by default.
    url  = config.get('database', {}).get('url', 'sqlite:///')
    
    base.BaseTest._session_maker = brownthrower.init(url)

def teardown():
    #base.BaseTest._session_maker.metadata.drop_all()
    pass
