#! /usr/bin/env python
# -*- coding: utf-8 -*-

import random

import brownthrower as bt

class BaseTest(object):
    
    _session_maker = None
    
    def __init__(self):
        pass
    
    def randint(self, lo=100000, hi=999999):
        return random.randint(lo, hi)
    
    def setup(self):
        self.session = self._session_maker()
        return self
    
    def teardown(self):
        self.session.rollback()

class ExampleJob(bt.Job):
    _bt_name = 'example'
