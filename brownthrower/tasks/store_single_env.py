#!/usr/bin/env python
# -*- coding: utf-8 -*-

from brownthrower import interface

class StoreSingleEnv(interface.Task):
    
    """\
    Stores an environment variable in a file.
    
    The objective of this Job is to store a single environment variable inside a
    file. It receives two parameters:
        'key'     The name of the environment variable to be stored
        'path'    Full path of the file to store the environment variable.
                  The file will be created if it does not exist, or truncated if
                  it already exists.
    """
    
    config_schema = """\
    {
        "type"                 : "object",
        "$schema"              : "http://json-schema.org/draft-03/schema",
        "required"             : true,
        "additionalProperties" : false,
        "properties": {
            "key": {
                "type"     : "string",
                "required" : true
            }, 
            "path": {
                "type"     : "string",
                "required" : true
            }
        }
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
        "type"     : "null",
        "$schema"  : "http://json-schema.org/draft-03/schema",
        "required" : true
    }
    """
    
    config_sample = """\
        # Environment variable to be printed
        key: "PWD"
        
        # Path to the output file where the environment variable has to be printed
        path: "/tmp/output.txt"
    """
    
    input_sample = """\
        # Nothing is required for this job.
        []
    """
    
    output_sample ="""\
        # This job returns nothing.
    """ 
    
    @classmethod
    def run(cls, runner, config, inp):
        import os
        f = open(config['path'], "w")
        f.write("%s" % os.environ[config['key']])
        f.close()
