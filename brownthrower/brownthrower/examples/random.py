#!/usr/bin/env python
# -*- coding: utf-8 -*-

import brownthrower

class Random(brownthrower.Task):
    """
    Return a random float.
    
    Example class that returns a random floating-point number in the range
    [0.0, 1.0).
    """
    
    _bt_name = 'random'
    
    def run(self):
        import random
        
        print self.config
        print self.input
        return random.random()