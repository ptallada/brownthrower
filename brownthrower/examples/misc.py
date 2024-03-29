#!/usr/bin/env python
# -*- coding: utf-8 -*-

import brownthrower as bt

class Pipe(bt.Task):
    """\
    Return the received input.
    
    Example task that returns a the received input as output, without any transformation.
    """
    
    _bt_name = 'pipe'
    
    @classmethod
    def run(cls, job):
        return job.get_input()

class Sleep(bt.Task):
    """\
    Sleep an determined amount of seconds.
    
    Example task that takes an integer as a input and sleeps that amount of seconds.
    
    Example input: 5
    """
    
    _bt_name = 'sleep'
    
    @classmethod
    def run(cls, job):
        import time
        
        time.sleep(job.get_input())

class Environ(bt.Task):
    """\
    Get the current environment.
    
    Example task that takes returns the current environment as output
    """
    
    _bt_name = 'environ'
    
    @classmethod
    def run(cls, job):
        import os
        
        return os.environ
