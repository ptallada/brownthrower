#! /usr/bin/env python
# -*- coding: utf-8 -*-

from brownthrower import interface

class Noop(interface.Chain):
    """\
    Return the same input as the output.
    
    This chain generates a single 'misc.noop' task.
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
        "type"     : "any",
        "$schema"  : "http://json-schema.org/draft-03/schema",
        "required" : true
    }
    """
    
    output_schema = """\
    {
        "type"     : "any",
        "$schema"  : "http://json-schema.org/draft-03/schema",
        "required" : true
    }
    """
    
    config_sample = """\
        # Nothing is required for this job.
    """
    
    input_sample = """\
        # Anything
        42
    """
    
    output_sample = """\
        # The supplied input
        42
    """
    
    def prolog(self, tasks, inp):
        task = tasks['misc.noop']
        
        return ((inp, task()))
    
    def epilog(self, clusters, out):
        return out[0]
