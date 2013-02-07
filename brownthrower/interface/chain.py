#! /usr/bin/env python
# -*- coding: utf-8 -*-

import json
import jsonschema
import textwrap
import yaml

class ChainValidationException(Exception):
    def __init__(self, message=None, exception=None):
        self.exception = exception
        self.message   = message
        
    def __str__(self):
        return "%s: %s" % (self.message, repr(self.exception))

class BaseChain(object):
    
    @property
    def config(self):
        return self._config
    
    @config.setter
    def config(self, config):
        try:
            jsonschema.validate(config, json.loads(self.config_schema))
            self.check_config(config)
        except Exception as e:
            raise ChainValidationException('Config is not valid', e)
        else:
            self._config = config
    
    @classmethod
    def validate_config(cls, config):
        try:
            config = yaml.safe_load(config)
            jsonschema.validate(config, json.loads(cls.config_schema))
            cls.check_config(config)
        except Exception as e:
            raise ChainValidationException('Config is not valid', e)
    
    @classmethod
    def validate_input(cls, inp):
        try:
            inp = yaml.safe_load(inp)
            jsonschema.validate(inp, json.loads(cls.input_schema))
            cls.check_input(inp)
        except Exception as e:
            raise ChainValidationException('Input is not valid', e)
    
    @classmethod
    def validate_output(cls, out):
        try:
            out = yaml.safe_load(out)
            jsonschema.validate(out, json.loads(cls.output_schema))
            cls.check_output(out)
        except Exception as e:
            raise ChainValidationException('Output is not valid', e)
    
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

class Chain(BaseChain):
    """\
    This MUST be a single line describing the objective of this Chain.
    
    The following line MUST be a more detailed description of this Chain, what
    parameters does it take, what are its configuration values and what kind of
    output it generates.
    """
    
    def __init__(self, config):
        """
        Create a new instance of this Chain. The instantiation will only
        succeed if the supplied config is valid and passes the additional
        checks (if present)
        
        @param config: mapping with the required configuration values
        @type config: dict
        """
        self.config = config
    
    def prolog(self, tasks, inp):
        """
        Prepares this Chain for execution. When this method is called, it can
        safely assume that the 'inp' parameter has been checked previously and
        that it is valid.
        
        Return a list of tuples. Each one of this tuples contains two Task
        instances '(parent_task, child_task)' and represents the parent-child
        dependency between them.
        Initial tasks, which do not have any parent, MUST have its input
        instead of a parent.
        
        @param tasks: mapping with all the registered Tasks available
        @type tasks: dict
        @param inp:  mapping with the output of the parent chains
        @type inp: dict
        @return: a list of tuples representing the dependency between Tasks or Chains
        @rtype: list
        """
        raise NotImplementedError
    
    def epilog(self, clusters, out):
        """
        Generate the final output of this Chain. When this method is called,
        it can safely assume that the 'config' and 'out' parameters have been
        checked previously and that they are both valid.
        
        @param config: mapping with the required configuration values
        @type config: dict
        @param out:  mapping with the output of the leaf Jobs
        @type out: dict
        @return: mapping to be delivered as output for child chains
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
    def check_config(cls, config):
        """
        Additional checks to the supplied config, after it has passed the schema
        validation.
        
        @param config: mapping with configuration values
        @type config: dict
        @return: None if the config is valid, or any Exception if not.
        """
        pass
    
    @classmethod
    def check_input(cls, inp):
        """
        Additional checks to the supplied input, after it has passed the schema
        validation.
        
        @param inp: mapping with input data
        @type inp: dict
        @return: None if the input is valid, or any Exception if not.
        """
        pass
    
    @classmethod
    def check_output(cls, out):
        """
        Additional checks to the supplied output, after it has passed the schema
        validation.
        
        @param out: mapping with output data
        @type out: dict
        @return: None if the output is valid, or any Exception if not.
        """
        pass
