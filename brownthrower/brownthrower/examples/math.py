#!/usr/bin/env python
# -*- coding: utf-8 -*-

import brownthrower as bt

class Random(bt.Task):
    """
    Return a random float.
    
    Example class that returns a random floating-point number in the range
    [0.0, 1.0).
    """
    
    _bt_name = 'random'
    
    @classmethod
    def run(cls, job):
        import random
        
        print job.get_config()
        print job.get_input()
        return random.random()