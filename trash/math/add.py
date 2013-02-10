#!/usr/bin/env python
# -*- coding: utf-8 -*-

from brownthrower import interface

class Add(interface.Task):
    """\
    Add the two of its inputs.
    
    This task returns the integer addition of its two inputs.
    It does not require any configuration.
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
        "maxItems" : 2,
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
        # An array with two integers
        [ 2, 3 ]
    """
    
    output_sample = """\
        # The sum of the input
        42
    """
    
    def run(self, runner, inp):
        return inp[0] + inp[1]
