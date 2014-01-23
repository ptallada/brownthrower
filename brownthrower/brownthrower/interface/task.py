#! /usr/bin/env python
# -*- coding: utf-8 -*-

class CancelledException(Exception):
    pass

class Task(object):
    """\
    Expected interface for the Task objects.
    
    This class represents the interface that the brownthrower framework expects
    when dealing with Tasks.
    
    Each Task has a 'name' attribute which must return a globally unique
    identifier for this task. All available tasks in a specific brownthrower
    environment will have to be distinguished by this name.
    
    When the Task is executed, the 'prolog' method is called to deploy the
    subjobs. If the 'prolog' method returns a set of subjobs, this Task will
    enter the PROCESSING state. When all its subjobs have finished successfully,
    its 'epilog' method will be called to generate the final output and,
    optionally, a new set of child jobs.
    
    If the 'prolog' is not implemented or it does not return any subjob, this
    Task will enter the PROCESSING state and its 'run' method will be called.
    The output of this Task will be the output of the 'run' method. Please, note
    that if the 'prolog' method is not implemented of it does not return any
    subjob, the 'epilog' method will not be called.
    
    The doctring of every Task class is used as the internal help for the
    manager interface. The first line MUST be a short description of the Task.
    The following lines MUST be a more detailed description of the Task, what
    parameters does it take, what are its configuration values and what kind of
    output it generates.
    """
    
    # Global unique identifying name for this task
    __brownthrower_name__   = "task.name"
    
    # Name of the runner required to run this Task. OPTIONAL.
    __brownthrower_runner__ = "runner.name"
    
    def __init__(self, config):
        """
        Create a new instance of this Task. The instantiation will only succeed
        if the supplied config is valid and passes the additional checks (if
        present)
        
        @param config: mapping with the required configuration values
        @type config: dict
        """
        self.config = config
    
    def prolog(self, tasks, inp, job_id = None):
        """
        Prepares this Job for execution. When this method is called, it can
        safely assume that the 'inp' parameter has been checked previously and
        that it is valid.
        
        Return a mapping with the following structure, or None if no subjobs are
        required:
        
            {
                'subjobs' : {
                    Task_A(config),
                    Task_B(config),
                    Task_B(config),
                },
                'input' : {
                    task_M : <input>,
                    task_N : <input>,
                }
                'links' : [
                    ( task_X, task_Y ),
                ]
            }
        
        The 'subjobs' entry is a set, with each element being a Task instance
        with its associated config.
        
        The 'input' entry is also a mapping, indexed by any of the keys present
        in the 'subjobs' dictionary, and its value is the input of that Task.
        
        Finally, the 'links' entry contains a list of tuples. Each tuple
        contains two Task instances (which must be present in the 'subjobs'
        mapping) representing the parent and the child sides, respectively, of a
        parent-child dependency.
        
        @param tasks: mapping with all the registered Tasks available
        @type tasks: dict
        @param inp:  list with the output of the parent jobs
        @type inp: dict
        @param job_id: unique identifier for this job
        @type job_id: int
        @return: a mapping representing the structure of the subjobs
        @rtype: mapping
        """
        raise NotImplementedError
    
    def epilog(self, tasks, out, job_id = None):
        """
        Wraps up this Task for the end of its processing. When this method is
        called, it can safely assume that the 'config' and 'out' parameters have
        been checked previously and that they are both valid.
        
        Return a mapping with the following structure:
        
            {
                'children' : {
                    Task_A(config),
                    Task_B(config),
                    Task_B(config),
                },
                'links' : [
                    ( task_X, task_Y ),
                ]
                'output' : <output>
            }
        
        The 'children' entry is a set, with each key being a Task instance
        with its associated config.
        
        The 'links' entry contains a list of tuples. Each tuple contains two
        Task instances (which must be present in the 'children' mapping)
        representing the parent and the child sides, respectively, of a parent-
        child dependency.
        
        Finally, the 'output' entry contains the final output of this Job.
        
        @param config: mapping with the required configuration values
        @type config: dict
        @param out:  mapping with the output of the leaf Jobs
        @type out: dict
        @param job_id: unique identifier for this job
        @type job_id: int
        @return: a mapping with the output and the structure of the child jobs
        @rtype: dict
        """
        raise NotImplementedError
    
    def run(self, inp, job_id):
        """
        Executes this Task. When this method is called, it can safely assume
        that the 'inp' parameter has been checked previously and that it is
        valid.
        
        @param inp:  list with the output of the parent jobs
        @type inp: list
        @param job_id: unique identifier for this job
        @type job_id: int
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
        Return a working configuration sample, that may be used as default.
        
        Users are expected to use these settings and only override those that do
        not fit their needs.
        
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
        validation. This method is OPTIONAL.
        
        @param config: mapping with configuration values
        @type config: dict
        @return: None if the config is valid, or any Exception if not.
        """
        pass
    
    @classmethod
    def check_input(cls, inp):
        """
        Additional checks to the supplied input, after it has passed the schema
        validation. This method is OPTIONAL.
        
        @param inp: mapping with input data
        @type inp: dict
        @return: None if the input is valid, or any Exception if not.
        """
        pass
    
    @classmethod
    def check_output(cls, out):
        """
        Additional checks to the supplied output, after it has passed the schema
        validation. This method is OPTIONAL.
        
        @param out: mapping with output data
        @type out: dict
        @return: None if the output is valid, or any Exception if not.
        """
        pass
