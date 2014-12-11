#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

from . import model
from . import utils

log = logging.getLogger('brownthrower.task')

class Task(object):
    """\
    Base class for user-defined Tasks.
    
    All Task subclasses must inherit from this class and override, as needed,
    the following methods:
     * :meth:`prolog`
     * :meth:`epilog`
     * :meth:`run`
    """
    
    _bt_name = None
    
    @utils.deprecated
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
        
        This code is run over a read-only transaction, so no modifications of
        any kind are allowed on the job database. If this task has to be
        decomposed in several subtasks, they must be created, configured (and
        optionally submitted) and appended to job.new_subjobs set.
        
        @param job: corresponding job for this task
        @type job: brownthrower.Job
        """
        pass
    
    @classmethod
    def epilog(cls, job):
        """
        Wraps up this Task for the end of its processing.
        
        This code is run over a read-only transaction, so no modifications of
        any kind are allowed on the job database. Is this task has to be
        followed by additional child tasks, they must be created, configured
        (and optionally submitted) and appended to job.new_children set.
        """
        pass
    
    @classmethod
    def run(cls, job):
        """
        Executes this Task.
        
        This code is run over a read-only transaction, so no modifications of
        any kind are allowed on the job database.
        
        @param inp:  list with the output of the parent jobs
        @type inp: list
        @param job_id: unique identifier for this job
        @type job_id: int
        @return: output to be delivered as input for child jobs
        """
        pass
