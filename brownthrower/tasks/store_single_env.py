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

_help = (
    """\
    Stores an environment variable in a file.""",
    """\
    The objective of this Job is to store a single environment variable inside a file.
    It receives two parameters:
        'key'     The name of the environment variable to be stored
        'path'    Full path of the file to store the environment variable.
                  The file will be created if it does not exist, or truncated if it already exists.
    """)

def check_arguments(config):
    jsonschema.validate(config, _schema)

def get_template():
    return textwrap.dedent(_sample)

def get_help():
    return (textwrap.dedent(_help[0]), textwrap.dedent(_help[1]))

def run(config, runner):
    check_arguments(config)
    
    import os
    f = open(config['path'], "w")
    f.write("%s" % os.environ[config['key']])
    f.close()
