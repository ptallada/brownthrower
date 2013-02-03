#!/usr/bin/env python
# -*- coding: utf-8 -*-

from brownthrower import interface

class Hostname(interface.Task):
    """\
    Returns the hostname of the execution host.
    
    This job gets the hostname of the host in which it has been executed and
    returns it as its result. It does not require any parameter.
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
        "maxItems" : 0
    }
    """
    
    output_schema = """\
    {
        "type"                 : "object",
        "$schema"              : "http://json-schema.org/draft-03/schema",
        "required"             : true,
        "additionalProperties" : false,
        "properties": {
            "hostname": {
                "type"     : "string",
                "required" : true
            }
        }
    }
    """
    
    config_sample = """\
        # Nothing is required for this job.
    """
    
    input_sample = """\
        # Nothing is required for this job.
        []
    """
    
    output_sample = """\
        # Hostname of the execution host
        hostname : test.pau.pic.es
    """
    
    @classmethod
    def run(cls, runner, config, inp):
        import socket
        
        return { 'hostname' : socket.gethostname() }
