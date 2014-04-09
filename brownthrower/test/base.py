#! /usr/bin/env python
# -*- coding: utf-8 -*-

import contextlib
import random

import brownthrower as bt

class BaseTest(object):
    
    _session_maker = None
    _use_session = False
    
    def __init__(self):
        pass
    
    def randint(self, lo=100000, hi=999999):
        return random.randint(lo, hi)
    
    def setup(self):
        #print "BASE SETUP %d" % id(self)
        pass
    
    def teardown(self):
        #print "BASE TEARDOWN %d" % id(self)
        self.session.rollback()
    
    @property
    def session(self):
        return self._session_maker()
    
    @contextlib.contextmanager
    def in_session(self, instances):
        if self._use_session:
            self.session.add_all(instances)
            self.session.flush()
            self.session.expire_all()
        
        yield self.session

class ExampleTask(bt.Task):
    _bt_name = 'example'
