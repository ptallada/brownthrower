#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import textwrap

from .api import Job

log = logging.getLogger('brownthrower.interface')

class DocumentedTask(type):
    @property
    def summary(self):
        return self.__doc__.strip().split('\n')[0].strip()
    
    @property
    def description(self):
        return textwrap.dedent('\n'.join(self.__doc__.strip().split('\n')[1:])).strip()

class BaseTask(object):
    __metaclass__ = DocumentedTask
    
    _bt_name = None
    
    def __new__(cls, *args, **kwargs):
        job = Job._init(task = cls._bt_name)
        job._impl = cls
        return job

class Task(BaseTask):
    """
    Base class for user-defined Tasks.
    
    All Task subclasses must inherit from this class and override the methods
    defined in brownthrower.interface.Task.
    """
    
    @classmethod
    def prolog(cls, job):
        """
        Prepares this Task for execution.
        
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
        
        @param job: corresponding job for this task
        @type job: brownthrower.Job
        @return: a mapping representing the structure of the subjobs
        @rtype: mapping
        """
        pass
    
    @classmethod
    def epilog(cls, job):
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
        pass
    
    @classmethod
    def run(cls, job):
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
        pass
    
    @property
    def config_sample(self):
        """
        Return a working configuration sample, that may be used as default.
        
        Users are expected to use these settings and only override those that do
        not fit their needs.
        
        @return: A YAML string containing the requested sample
        @rtype: basestring
        """
        pass
    
    @property
    def input_sample(self):
        """
        Return a working input sample.
        
        @return: A YAML string containing the requested sample
        @rtype: basestring
        """
        pass
    
    @property
    def output_sample(self):
        """
        Return a working output sample.
        
        @return: A YAML string containing the requested sample
        @rtype: basestring
        """
        pass
