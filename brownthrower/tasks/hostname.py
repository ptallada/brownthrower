#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import jsonschema
import textwrap
import yaml

from brownthrower import interface

class Hostname(interface.Task):
    
    _config_schema = """\
    {
        "type"     : "null",
        "$schema"  : "http://json-schema.org/draft-03/schema",
        "required" : true
    }
    """
    
    _input_schema = _config_schema
    
    _output_schema = """\
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
    
    _config_sample = """\
        # Nothing is required for this job.
    """
    
    _input_sample = _config_sample
    
    _output_sample = """\
        # Hostname of the execution host
        hostname : test.pau.pic.es
    """
    
    _help = (
        "Returns the hostname of the execution host.",
        """\
        This job gets the hostname of the host in which is been executed and returns it as its result.
        It does not require any parameter.
        """
    )
    
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
        import socket
        
        return { 'hostname' : socket.gethostname() }
