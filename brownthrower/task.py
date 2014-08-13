#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

from . import model

log = logging.getLogger('brownthrower.task')

class Task(object):
    """
    Base class for user-defined Tasks.
    
    All Task subclasses must inherit from this class and override, as needed,
    the following methods:
     * :meth:`prolog`
     * :meth:`epilog`
     * :meth:`run`
    """
    
    _bt_name = None
    
    @model._deprecated
    def __init__(self, config):
        self.config = config
    
    @classmethod
    def create_job(cls):
        """\
        Create a :class:`Job` instance corresponding to this Task.
        """
        return model.Job(name = cls._bt_name, task = cls)
    
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
