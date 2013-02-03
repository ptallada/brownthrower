#! /usr/bin/env python
# -*- coding: utf-8 -*-

from brownthrower import interface

class Sum(interface.Event):
    """\
    Calculate the sum of the input.
    
    Return the sum of all the inputs. This Event generates its output building
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
        "minItems" : 2,
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
        # An unbounded array of integers
        [ 1, 2, 3, 4 ]
    """
    
    output_sample = """\
        # The sum of the input
        42
    """
    
    def prolog(self, tasks, config, inp):
        task = tasks['math.add']
        dependencies = []
        
        dependencies.append(())
    
    def epilog(self, config, out):
        pass