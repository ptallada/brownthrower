#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import json
import jsonschema
import pkg_resources
import textwrap
import yaml

from brownthrower import interface
from brownthrower import model
from brownthrower.interface import constants

log = logging.getLogger('brownthrower.api')

class InvalidStatusException(Exception):
    def __init__(self, message=None):
        self.message = message
        
    def __str__(self):
        return str(self.message)

def validate_config(task, config):
    try:
        config = yaml.safe_load(config)
        jsonschema.validate(config, json.loads(task.config_schema))
        task.check_config(config)
    except Exception as e:
        raise interface.TaskValidationException('Config is not valid', e)

def validate_input(task, inp):
    try:
        inp = yaml.safe_load(inp)
        jsonschema.validate(inp, json.loads(task.input_schema))
        task.check_input(inp)
    except Exception as e:
        raise interface.TaskValidationException('Input is not valid', e)

def validate_output(task, out):
    try:
        out = yaml.safe_load(out)
        jsonschema.validate(out, json.loads(task.output_schema))
        task.check_output(out)
    except Exception as e:
        raise interface.TaskValidationException('Output is not valid', e)

def get_config_schema(task):
    return textwrap.dedent(task.config_schema).strip()

def get_input_schema(task):
    return textwrap.dedent(task.input_schema).strip()

def get_output_schema(task):
    return textwrap.dedent(task.output_schema).strip()

def get_config_sample(task):
    return textwrap.dedent(task.config_sample).strip()

def get_input_sample(task):
    return textwrap.dedent(task.input_sample).strip()

def get_output_sample(task):
    return textwrap.dedent(task.output_sample).strip()

def get_help(task):
    doc = task.__doc__.strip().split('\n')
    short = doc[0].strip()
    detail = textwrap.dedent('\n'.join(doc[1:])).strip()
    
    return (short, detail)

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
            
            assert len(get_help(task)[0]) > 0
            assert len(get_help(task)[1]) > 0
            
            validate_config(task, task.config_sample)
            validate_input( task, task.input_sample)
            validate_output(task, task.output_sample)
            
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
        
        validate_config(task, job.config)
        if not job.parents:
            validate_input(task, job.input)
    
    job.submit()
    
    for ancestor in ancestors:
        ancestor.update_status()
    
    model.session.commit()
