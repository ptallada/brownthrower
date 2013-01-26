#!/usr/bin/env python
# -*- coding: utf-8 -*-

import jsonschema
import textwrap

_schema = {
    "type": "object", 
    "$schema": "http://json-schema.org/draft-03/schema", 
    "required": True, 
    "additionalProperties": False, 
    "properties": {
        "key": {
            "type": "string", 
            "required": True
        }, 
        "path": {
            "type": "string", 
            "required": True
        }
    }
}

_sample = """\
    # Environment variable to be printed
    key: "LD_LIBRARY_PATH"
    
    # Path to the output file where the environment variable has to be printed
    path: "output.txt"
"""

_help = "Stores an environment variable in a file."

def check_arguments(config):
    jsonschema.validate(config, _schema)

def get_template():
    return textwrap.dedent(_sample)

def get_help():
    return _help

def run(config, runner):
    check_arguments(config)
    
    import os
    f = open(config['path'], "w")
    f.write("%s" % os.environ[config['key']])
    f.close()
