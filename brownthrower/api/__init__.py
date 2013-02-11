#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import pkg_resources

from brownthrower import interface
from brownthrower import model
from brownthrower.interface import constants

log = logging.getLogger('brownthrower.api')

class InvalidStatusException(Exception):
    def __init__(self, message=None):
        self.message = message
        
    def __str__(self):
        return str(self.message)

def load_tasks(entry_point):
    """
    Build a list with all the Tasks available in the current environment.
    """
    
    tasks = {}
    
    for entry in pkg_resources.iter_entry_points(entry_point):
        try:
            task = entry.load()
            
            assert isinstance(task.config_schema, basestring)
            assert isinstance(task.input_schema,  basestring)
            assert isinstance(task.output_schema, basestring)
            
            assert isinstance(task.config_sample, basestring)
            assert isinstance(task.input_sample,  basestring)
            assert isinstance(task.output_sample, basestring)
            
            assert len(task.get_help()[0]) > 0
            assert len(task.get_help()[1]) > 0
            assert '\n' not in task.get_help()[0]
            
            task.validate_config(task.config_sample)
            task.validate_input( task.input_sample)
            task.validate_output(task.output_sample)
            
            if entry.name in tasks:
                log.warning("Skipping Task '%s:%s': a Task with the same name is already defined." % (entry.name, entry.module_name))
                continue
            
            tasks[entry.name] = task
        
        except (AttributeError, AssertionError) as e:
            log.warning("Skipping Task '%s:%s': it does not properly implement the interface." % (entry.name, entry.module_name))
            log.debug("Task '%s:%s': %s" % (entry.name, entry.module_name, e))
        except interface.TaskValidationException as e:
            log.warning("Skipping Task '%s:%s': their own samples fail to validate." % (entry.name, entry.module_name))
            log.debug("Task '%s:%s': %s" % (entry.name, entry.module_name, e))
        except ImportError:
            log.warning("Skipping Task '%s:%s': unable to load." % (entry.name, entry.module_name))
    
    return tasks

def load_dispatchers(entry_point):
    """
    Build a list with all the Dispatchers available in the current environment.
    """
    
    dispatchers = {}
    
    for entry in pkg_resources.iter_entry_points(entry_point):
        try:
            dispatcher = entry.load()
            
            assert len(dispatcher.get_help()[0]) > 0
            assert len(dispatcher.get_help()[1]) > 0
            assert '\n' not in dispatcher.get_help()[0]
            
            if entry.name in dispatchers:
                log.warning("Skipping Dispatcher '%s:%s': a Dispatcher with the same name is already defined." % (entry.name, entry.module_name))
                continue
            
            dispatchers[entry.name] = dispatcher
        
        except (AttributeError, AssertionError) as e:
            log.warning("Skipping Dispatcher '%s:%s': it does not properly implement the interface." % (entry.name, entry.module_name))
            log.debug("Dispatcher '%s:%s': %s" % (entry.name, entry.module_name, e))
        except ImportError:
            log.warning("Skipping Dispatcher '%s:%s': unable to load." % (entry.name, entry.module_name))
    
    return dispatchers

def submit(job_id, tasks):
    ancestors = model.helper.ancestors(job_id, lockmode='update')
    
    job = ancestors[0]
    
    if job.status not in [
        constants.JobStatus.STASHED,
        constants.JobStatus.CANCELLED,
        constants.JobStatus.FAILED,
        constants.JobStatus.PROLOG_FAIL,
        constants.JobStatus.EPILOG_FAIL,
    ]:
        raise InvalidStatusException("The job cannot be submitted in its current status.")
    
    if job.status == constants.JobStatus.STASHED:
        task = tasks.get(job.task)
        if not task:
            raise interface.TaskUnavailableException(job.task)
        
        task.validate_config(job.config)
        if not job.parents:
            task.validate_input(job.input)
    
    job.submit()
    
    for ancestor in ancestors:
        ancestor.update_status()
