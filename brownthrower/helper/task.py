#! /usr/bin/env python
# -*- coding: utf-8 -*-

import json
import jsonschema
import textwrap
import yaml

from brownthrower.interface.task import TaskValidationException

class BaseTask(object):
    
    @classmethod
    def validate_config(cls, config):
        try:
            config = yaml.safe_load(config)
            jsonschema.validate(config, json.loads(cls.config_schema))
            cls.check_config(config)
        except Exception as e:
            raise TaskValidationException('Config is not valid', e)
    
    @classmethod
    def validate_input(cls, inp):
        try:
            inp = yaml.safe_load(inp)
            jsonschema.validate(inp, json.loads(cls.input_schema))
            cls.check_input(inp)
        except Exception as e:
            raise TaskValidationException('Input is not valid', e)
    
    @classmethod
    def validate_output(cls, out):
        try:
            out = yaml.safe_load(out)
            jsonschema.validate(out, json.loads(cls.output_schema))
            cls.check_output(out)
        except Exception as e:
            raise TaskValidationException('Output is not valid', e)
    
    @classmethod
    def get_config_schema(cls):
        return textwrap.dedent(cls.config_schema).strip()
    
    @classmethod
    def get_input_schema(cls):
        return textwrap.dedent(cls.input_schema).strip()
    
    @classmethod
    def get_output_schema(cls):
        return textwrap.dedent(cls.output_schema).strip()
    
    @classmethod
    def get_config_sample(cls):
        return textwrap.dedent(cls.config_sample).strip()
    
    @classmethod
    def get_input_sample(cls):
        return textwrap.dedent(cls.input_sample).strip()
    
    @classmethod
    def get_output_sample(cls):
        return textwrap.dedent(cls.output_sample).strip()
    
    @classmethod
    def get_help(cls):
        doc = cls.__doc__.strip().split('\n')
        short = doc[0].strip()
        detail = textwrap.dedent('\n'.join(doc[1:])).strip()
        
        return (short, detail)
