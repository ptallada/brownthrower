#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import jsonschema
import textwrap
import yaml

from brownthrower.interfaces import Task

class StoreSingleEnv(Task):
    
    _config_schema = """\
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
    
    _input_schema = """\
    {
        "type"     : "null",
        "$schema"  : "http://json-schema.org/draft-03/schema",
        "required" : true
    }
    """
    
    _output_schema = _input_schema
    
    _config_sample = """\
        # Environment variable to be printed
        key: "LD_LIBRARY_PATH"
        
        # Path to the output file where the environment variable has to be printed
        path: "output.txt"
    """
    
    _input_sample = """\
        # Nothing is required for this job.
    """
    
    _output_sample = _input_sample
    
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
    
    @classmethod
    def check_config(cls, config):
        jsonschema.validate(yaml.load(config), json.loads(cls._config_schema))
    
    @classmethod
    def check_input(cls, inp):
        jsonschema.validate(yaml.load(inp), json.loads(cls._input_schema))
    
    @classmethod
    def check_output(cls, out):
        jsonschema.validate(yaml.load(out), json.loads(cls._output_schema))
    
    @classmethod
    def get_config_schema(cls):
        return textwrap.dedent(cls._config_schema)
    
    @classmethod
    def get_input_schema(cls):
        return textwrap.dedent(cls._input_schema)
    
    @classmethod
    def get_output_schema(cls):
        return textwrap.dedent(cls._output_schema)
    
    @classmethod
    def get_config_template(cls):
        return textwrap.dedent(cls._config_sample)
    
    @classmethod
    def get_input_template(cls):
        return textwrap.dedent(cls._input_sample)
    
    @classmethod
    def get_output_template(cls):
        return textwrap.dedent(cls._output_sample)
    
    @classmethod
    def get_help(cls):
        return (textwrap.dedent(cls._help[0]), textwrap.dedent(cls._help[1]))
    
    @classmethod
    def run(cls, runner, config, inp):
        import os
        f = open(config['path'], "w")
        f.write("%s" % os.environ[config['key']])
        f.close()
