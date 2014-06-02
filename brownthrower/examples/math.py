#!/usr/bin/env python
# -*- coding: utf-8 -*-

import brownthrower as bt

class Random(bt.Task):
    """\
    Return a random float.
    
    Example task that returns a random floating-point number in the range
    [0.0, 1.0).
    """
    
    _bt_name = 'random'
    
    @classmethod
    def run(cls, job):
        import random
        
        return random.random()

class Add2(bt.Task):
    """\
    Add the two of its inputs.
    
    Example task that takes an array with two values and returns the result of
    adding them. It does not require any configuration.
    
    Example input: [12, 30]
    
    Output: 42
    """
    
    _bt_name = 'add2'
    
    @classmethod
    def run(cls, job):
        inp = job.get_input()
        return inp[0] + inp[1]

class Sum4(bt.Task):
    """\
    Calculate the sum of its four inputs.
    
    Example task that takes an array of four values and computes its sum, using
    Add2 tasks. It does not require any configuration.
    
    Example input: [5, 10, 12, 15]
    
    Output: 42
    """
    
    _bt_name = 'sum4'
    
    @classmethod
    def prolog(self, job):
        inp = job.get_input()
        
        s1 = Add2()
        s1.set_input(inp[:2])
        
        s2 = Add2()
        s2.set_input(inp[2:])
        
        s3 = Add2()
        s3.parents |= set([s1, s2])
        
        job.subjobs |= set([s1, s2, s3])
    
    @classmethod
    def epilog(cls, job):
        for j in job.subjobs:
            if j.parents:
                return j.get_output()
