#! /usr/bin/env python
# -*- coding: utf-8 -*-

class Task(object):
    
    def run(self, runner, config, inp={}):
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
    
    def check_config(self, config):
        """
        Check if the supplied config suites the required schema.
        
        @param config: YAML string with configuration values
        @type config: basestring
        @return: None if the config is valid, or some Exception if not.
        """
        raise NotImplementedError
    
    def check_input(self, inp):
        """
        Check if the supplied input suites the required schema.
        
        @param input: YAML string with input data
        @type input: basestring
        @return: None if the input is valid, or some Exception if not.
        """
        raise NotImplementedError
    
    def check_output(self, out):
        """
        Check if the supplied output suites the required schema.
        
        @param output: YAML string with output data.
        @type output: basestring
        @return: None if the output is valid, or some Exception if not.
        """
        raise NotImplementedError
    
    def get_config_schema(self):
        """
        Return the configuration formal JSON schema.
        
        @return: A string containing the requested schema
        @rtype: basestring
        """
        raise NotImplementedError
    
    def get_input_schema(self):
        """
        Return the input formal JSON schema.
        
        @return: A string containing the requested schema
        @rtype: basestring
        """
        raise NotImplementedError
    
    def get_output_schema(self):
        """
        Return the output formal JSON schema.
        
        @return: A string containing the requested schema
        @rtype: basestring
        """
        raise NotImplementedError
    
    def get_config_template(self):
        """
        Return a working configuration sample.
        
        @return: A YAML string containing the requested template
        @rtype: basestring
        """
        raise NotImplementedError
    
    def get_input_template(self):
        """
        Return a working input sample.
        
        @return: A YAML string containing the requested template
        @rtype: basestring
        """
        raise NotImplementedError
    
    def get_output_template(self):
        """
        Return a working output sample.
        
        @return: A YAML string containing the requested template
        @rtype: basestring
        """
        raise NotImplementedError
    
    def get_help(self):
        """
        Return a tuple with two elements.
        The first one MUST be a short description in a single line.
        The second one MUST be a long description that may span multiple lines.
        
        @return: A short and a longer description of this task
        @rtype: tuple 
        """
        raise NotImplementedError
