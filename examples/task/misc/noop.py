#! /usr/bin/env python
# -*- coding: utf-8 -*-

from brownthrower import interface

class Noop(interface.Task):
    """\
    No-operation Task. Pipe the input as the output.
    
    This task allows jobs to provide additional input to other jobs that already
    have some parents defined. Returns the input as the output.
    """
    
    name = 'misc.noop'
    
    config_schema = """\
    {
        "type"     : "null",
        "$schema"  : "http://json-schema.org/draft-03/schema#",
        "required" : true
    }
    """
    
    input_schema = """\
    {
        "type"     : "any",
        "$schema"  : "http://json-schema.org/draft-03/schema#",
        "required" : true
    }
    """
    
    output_schema = """\
    {
        "type"     : "any",
        "$schema"  : "http://json-schema.org/draft-03/schema#",
        "required" : true
    }
    """
    
    config_sample = """\
        # Nothing is required for this job.
    """
    
    input_sample = """\
        # Any valid JSON structure is accepted
        42
    """
    
    output_sample = """\
        # The received input
        42
    """
    
    def run(self, runner, inp):
        return inp
