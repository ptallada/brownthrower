#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import json
import jsonschema
import pkg_resources
import textwrap
import yaml

from brownthrower import interface, model
from brownthrower.interface import constants

_tasks = {}

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

def init(entry_point):
    """\
    Build a list with all the Tasks available in the current environment.
    """
    global _tasks
    
    for entry in pkg_resources.iter_entry_points(entry_point):
        try:
            task = entry.load()
            
            assert isinstance(task.name,          basestring)
            
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
            
            if task.name in _tasks:
                log.warning("Skipping duplicate Task '%s' from '%s:%s'." % (task.name, entry.name, entry.module_name))
                continue
            
            _tasks[task.name] = task
            
        except Exception as e:
            try:
                raise
            except (AttributeError, AssertionError):
                log.warning("Task '%s:%s' does not properly implement the interface." % (entry.name, entry.module_name))
            except interface.TaskValidationException:
                log.warning("Samples from Task '%s:%s' are not valid." % (entry.name, entry.module_name))
            except ImportError:
                log.warning("Unable to load Task '%s:%s'." % (entry.name, entry.module_name))
            finally:
                log.debug(e)

def get_tasks():
    global _tasks
    return _tasks

def get_task(name):
    global _tasks
    return _tasks[name]

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
        except ImportError as e:
            log.warning("Skipping Dispatcher '%s:%s': unable to load." % (entry.name, entry.module_name))
    
    return dispatchers

def create(name):
    session = model.session_maker()
    
    task = get_task(name)
    
    job =  model.Job(
        task    = name,
        config  = get_config_sample(task),
        status  = constants.JobStatus.STASHED
    )
    
    session.add(job)
    session.flush()
    
    return job.id

def link(parent_id, child_id):
    session = model.session_maker()
    
    parent = session.query(model.Job).filter_by(
        id = parent_id,
    ).with_lockmode('read').one()
    
    child = session.query(model.Job).filter_by(
        id     = child_id,
    ).with_lockmode('read').one()
    
    if child.status != constants.JobStatus.STASHED:
        raise InvalidStatusException("The child job must be in the stash.")
        
    if parent.super_id or child.super_id:
        raise InvalidStatusException("A parent-child dependency can only be manually established between top-level jobs.")
        
    dependency = model.Dependency(
        child_job_id  = child.id,
        parent_job_id = parent.id
    )
    session.add(dependency)

def submit(job_id):
    session = model.session_maker()
    
    job = session.query(model.Job).filter_by(id = job_id).one()
    
    ancestors = job.ancestors(lockmode='update')[1:]
    
    if job.status not in [
        constants.JobStatus.STASHED,
        constants.JobStatus.CANCELLED,
        constants.JobStatus.FAILED,
    ]:
        raise InvalidStatusException("This job cannot be submitted in its current status.")
    
    if job.status == constants.JobStatus.STASHED:
        task = get_task(job.task)
        if not task:
            raise interface.TaskUnavailableException(job.task)
        
        validate_config(task, job.config)
        if not job.parents:
            validate_input(task, job.input)
    
    job.submit()
    
    for ancestor in ancestors:
        ancestor.update_status()

def remove(job_id):
    session = model.session_maker()
    
    job = session.query(model.Job).filter_by(id = job_id).with_lockmode('update').one()
    
    if job.super_id:
        raise InvalidStatusException("This job is not a top-level job and cannot be removed.")
    
    if job.status not in [
        constants.JobStatus.STASHED,
        constants.JobStatus.CANCELLED,
        constants.JobStatus.FAILED,
    ]:
        raise InvalidStatusException("This job cannot be removed in its current status.")
    
    if job.parents or job.children:
        raise InvalidStatusException("Cannot remove a linked job.")
    
    job.remove()

def reset(job_id):
    session = model.session_maker()
    
    job = session.query(model.Job).filter_by(id = job_id).with_lockmode('update').one()
    
    if job.super_id:
        raise InvalidStatusException("This job is not a top-level job and cannot be returned to the stash.")
    
    if job.subjobs:
        raise InvalidStatusException("This job already has subjobs and cannot be returned to the stash.")
    
    if job.status not in [
        constants.JobStatus.CANCELLED,
        constants.JobStatus.FAILED,
        constants.JobStatus.QUEUED,
    ]:
        raise InvalidStatusException("This job cannot be returned to the stash in its current status.")
    
    job.status     = constants.JobStatus.STASHED
    job.ts_queued  = None
    job.ts_started = None
    job.ts_ended   = None

def cancel(job_id):
    session = model.session_maker()
    
    job = session.query(model.Job).filter_by(id = job_id).one()
    
    ancestors = job.ancestors(lockmode='update')[1:]
    
    if job.status not in [
        constants.JobStatus.QUEUED,
        constants.JobStatus.PROCESSING,
        constants.JobStatus.FAILING,
    ]:
        raise InvalidStatusException("This job cannot be cancelled in its current status.")
    
    job.cancel()
    
    for ancestor in ancestors:
        ancestor.update_status()
