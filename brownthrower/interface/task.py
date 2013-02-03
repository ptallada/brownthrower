#! /usr/bin/env python
# -*- coding: utf-8 -*-

import json
import jsonschema
import textwrap
import yaml

class TaskValidationException(Exception):
    def __init__(self, message=None, exception=None, ):
        self.exception = exception
        self.message   = message
        
    def __str__(self):
        return "%s: %s" % (self.message, repr(self.exception))

class TaskCancelledException(Exception):
    pass

class BaseTask(object):
    @classmethod
    def validate_config(cls, config):
        try:
            jsonschema.validate(yaml.safe_load(config), json.loads(cls.config_schema))
            config = yaml.safe_load(config)
            cls.check_config(config)
        except Exception as e:
            raise TaskValidationException('Config is not valid', e)
    
    @classmethod
    def validate_input(cls, inp):
        try:
            jsonschema.validate(yaml.safe_load(inp), json.loads(cls.input_schema))
            inp = yaml.safe_load(inp)
            cls.check_input(inp)
        except Exception as e:
            raise TaskValidationException('Input is not valid', e)
    
    @classmethod
    def validate_output(cls, out):
        try:
            jsonschema.validate(yaml.safe_load(out), json.loads(cls.output_schema))
            out = yaml.safe_load(out)
            cls.check_output(out)
        except Exception as e:
            raise TaskValidationException('Output is not valid', e)
    
    @classmethod
    def get_config_schema(cls):
        return textwrap.dedent(cls.config_schema)
    
    @classmethod
    def get_input_schema(cls):
        return textwrap.dedent(cls.input_schema)
    
    @classmethod
    def get_output_schema(cls):
        return textwrap.dedent(cls.output_schema)
    
    @classmethod
    def get_config_sample(cls):
        return textwrap.dedent(cls.config_sample)
    
    @classmethod
    def get_input_sample(cls):
        return textwrap.dedent(cls.input_sample)
    
    @classmethod
    def get_output_sample(cls):
        return textwrap.dedent(cls.output_sample)
    
    @classmethod
    def get_help(cls):
        doc = cls.__doc__.strip().split('\n')
        short = doc[0].strip()
        detail = textwrap.dedent('\n'.join(doc[1:]))
        
        return (short, detail)

class Task(BaseTask):
    """\
    This MUST be a single line describing the objective of this Task.
    
    The following line MUST be a more detailed description of this Task, what
    parameters does it take, what are its configuration values and what kind of
    output it generates.
    """
    
    def run(self, runner, config, inp):
        """
        Executes this task. When this method is called, it can safely assume
        that the 'config' and 'inp' parameters have been checked previously
        and that they are both valid.
        
        @param runner: helper to abstract from the execution environment
        @type runner: L{Runner}
        @param config: mapping with the required configuration values
        @type config: dict
        @param inp:  mapping with the output of the parent jobs
        @type inp: dict
        @return: mapping to be delivered as input for child jobs
        @rtype: dict
        """
        raise NotImplementedError
    
    @property
    def config_schema(self):
        """
        Return the configuration formal JSON schema.
        
        @return: A string containing the requested schema
        @rtype: basestring
        """
        raise NotImplementedError
    
    @property
    def input_schema(self):
        """
        Return the input formal JSON schema.
        
        @return: A string containing the requested schema
        @rtype: basestring
        """
        raise NotImplementedError
    
    @property
    def output_schema(self):
        """
        Return the output formal JSON schema.
        
        @return: A string containing the requested schema
        @rtype: basestring
        """
        raise NotImplementedError
    
    @property
    def config_sample(self):
        """
        Return a working configuration sample.
        
        @return: A YAML string containing the requested sample
        @rtype: basestring
        """
        raise NotImplementedError
    
    @property
    def input_sample(self):
        """
        Return a working input sample.
        
        @return: A YAML string containing the requested sample
        @rtype: basestring
        """
        raise NotImplementedError
    
    @property
    def output_sample(self):
        """
        Return a working output sample.
        
        @return: A YAML string containing the requested sample
        @rtype: basestring
        """
        raise NotImplementedError
    
    @classmethod
    def check_config(self, config):
        """
        Additional checks to the supplied config, after it has passed the schema
        validation.
        
        @param config: mapping with configuration values
        @type config: dict
        @return: None if the config is valid, or any Exception if not.
        """
        pass
    
    @classmethod
    def check_input(self, inp):
        """
        Additional checks to the supplied input, after it has passed the schema
        validation.
        
        @param inp: mapping with input data
        @type inp: dict
        @return: None if the input is valid, or any Exception if not.
        """
        pass
    
    @classmethod
    def check_output(self, out):
        """
        Additional checks to the supplied output, after it has passed the schema
        validation.
        
        @param out: mapping with output data
        @type out: dict
        @return: None if the output is valid, or any Exception if not.
        """
        pass
