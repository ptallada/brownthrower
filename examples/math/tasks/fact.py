#!/usr/bin/env python
# -*- coding: utf-8 -*-

from brownthrower import interface

class Fact(interface.Task):
    """\
    Calculate the factorial of the input.
    
    This task returns the factorial of its input.
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
        "minItems" : 1,
        "maxItems" : 1,
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
        # An array with a single integer
        [ 3 ]
    """
    
    output_sample = """\
        # The sum of the input
        42
    """
    
    @classmethod
    def run(cls, runner, config, inp):
        import math
        return math.factorial(inp[0])
