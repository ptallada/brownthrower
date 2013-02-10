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
    
    def prolog(self, tasks, config, inp):
        task = tasks['math.add']
        
        t1 = task(config)
        t2 = task(config)
        t3 = task(config)
        
        return (
            (inp[0:1], t1),
            (inp[2:3], t2),
            (t1,       t3),
            (t2,       t3),
        )
        
    def epilog(self, config, out):
        return out[0]