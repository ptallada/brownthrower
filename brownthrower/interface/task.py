#! /usr/bin/env python
# -*- coding: utf-8 -*-

class TaskValidationException(Exception):
    def __init__(self, message=None, exception=None):
        self.exception = exception
        self.message   = message
        
    def __str__(self):
        return "%s: %s" % (self.message, repr(self.exception))

class TaskCancelledException(Exception):
    pass

class TaskUnavailableException(Exception):
    def __init__(self, task=None):
        self.task = task
        
    def __str__(self):
        return "Task '%s' is not available in this environment." % self.task

class Task(object):
    """\
    Expected interface for the Task objects.
    
    This class represents the interface that the brownthrower framework expects
    when dealing with Tasks. When the Task is executed, the 'prolog' method is
    called to deploy the subjobs.
    
    If the 'prolog' method returns a set of subjobs, this Task will enter the
    PROCESSING state. When all its subjobs have finished successfully, its
    'epilog' method will be called to generate the final output or a new set of
    child jobs.
    
    If the 'prolog' is not implemented or it returns None, this Task will enter
    the PROCESSING state and its 'run' method will be called. The output of this
    Task will be the output of the 'run' method. Please, note that if the
    'prolog' method is not implemented of it returns None, the 'epilog' method
    will not be called.
    
    The doctring of every Task class is used as the internal help for the
    manager interface. The first line MUST be a short description of the Task.
    The following lines MUST be a more detailed description of the Task, what
    parameters does it take, what are its configuration values and what kind of
    output it generates.
    """
    
    def __init__(self, config):
        """
        Create a new instance of this Task. The instantiation will only succeed
        if the supplied config is valid and passes the additional checks (if
        present)
        
        @param config: mapping with the required configuration values
        @type config: dict
        """
        self.config = config
    
    def prolog(self, tasks, inp):
        """
        Prepares this Job for execution. When this method is called, it can
        safely assume that the 'inp' parameter has been checked previously and
        that it is valid.
        
        Return a list of tuples. Each one of this tuples contains two Task
        instances '(parent_task, child_task)' and represents the parent-child
        dependency between them. Initial tasks, which do not have any parent,
        MUST have its input instead of a parent. Do not implement this method or
        return None if this Task shall not have any subjobs.
        
        @param tasks: mapping with all the registered Tasks available
        @type tasks: dict
        @param inp:  mapping with the output of the parent chains
        @type inp: dict
        @return: a list of tuples representing the dependency between Tasks
        @rtype: list
        """
        raise NotImplementedError
    
    def epilog(self, tasks, out):
        """
        Wraps up this Task for the end of its processing. When this method is
        called, it can safely assume that the 'config' and 'out' parameters have
        been checked previously and that they are both valid.
        
        Return a list of tuples, analogous as the 'prolog' method, that can be
        used to create a new set of child jobs which shall execute after this
        one. Each one of this tuples contains two Task instances '(parent_task,
        child_task)' and represents the parent-child dependency between them.
        Initial tasks, which do not have any parent, MUST have its input instead
        of a parent.
        
        In any case, the final output of this Task MUST be given with a pair
        '(output, None)'. If more than one tuple has None as its second element,
        the true value of the final ouput is unclear and an error will be
        raised.
        
        @param config: mapping with the required configuration values
        @type config: dict
        @param out:  mapping with the output of the leaf Jobs
        @type out: dict
        @return: a list of tuples representing the dependency between Tasks
        @rtype: list
        """
        raise NotImplementedError
    
    def run(self, runner, inp):
        """
        Executes this Task. When this method is called, it can safely assume
        that the 'inp' parameter has been checked previously and that it is
        valid.
        
        @param runner: helper to abstract from the execution environment
        @type runner: L{Runner}
        @param inp:  list with the output of the parent jobs
        @type inp: list
        @return: output to be delivered as input for child jobs
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
