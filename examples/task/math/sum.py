#! /usr/bin/env python
# -*- coding: utf-8 -*-

from brownthrower import interface

class Sum(interface.Task):
    """\
    Calculate the sum of the input.
    
    Return the sum of all the inputs. This Chain generates its output building
    a chain of 'add' Tasks.
    """
    
    config_schema = """\
    {
        "type"     : "null",
        "$schema"  : "http://json-schema.org/draft-03/schema",
        "required" : true
    }
    """
    
    input_schema = """\
    {
        "type"     : "array",
        "$schema"  : "http://json-schema.org/draft-03/schema",
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
        "type"                 : "integer",
        "$schema"              : "http://json-schema.org/draft-03/schema",
        "required"             : true
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
            'subjobs' : {
                t1 : 'math.add',
                t2 : 'math.add',
                t3 : 'math.add',
            },
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