#! /usr/bin/env python
# -*- coding: utf-8 -*-

from brownthrower.interface import task

class Sum(task.Task):
    """\
    Calculate the sum of the input.
    
    Return the sum of all the inputs. This Job generates its output building
    a set of of 'add' subjobs.
    """
    
    __brownthrower_name__   = 'math.sum'
    __brownthrower_runner__ = None
    
    config_schema = """\
    {
        "type"     : "null",
        "$schema"  : "http://json-schema.org/draft-03/schema#",
        "required" : true
    }
    """
    
    input_schema = """\
    {
        "type"     : "array",
        "$schema"  : "http://json-schema.org/draft-03/schema#",
        "required" : true,
        "minItems" : 4,
        "maxItems" : 4,
        "items"    : {
            "type"     : "integer",
            "required" : true
        }
    }
    """
    
    output_schema = """\
    {
        "type"     : "integer",
        "$schema"  : "http://json-schema.org/draft-03/schema#",
        "required" : true
    }
    """
    
    config_sample = """\
        # Nothing is required for this job.
    """
    
    input_sample = """\
        # An array of four integers
        [ 1, 2, 3, 4 ]
    """
    
    output_sample = """\
        # The sum of the input
        42
    """
    
    def prolog(self, tasks, inp):
        task = tasks['math.add']
        
        t1 = task(self.config)
        t2 = task(self.config)
        t3 = task(self.config)
        
        return {
            'subjobs' : set([
                t1,
                t2,
                t3,
            ]),
            'input' : {
                t1 : inp[0:2],
                t2 : inp[2:4],
            },
            'links' : (
                (t1, t3),
                (t2, t3),
            )
        }
        
    def epilog(self, tasks, out):
        return {
            'output' : out[0],
        }