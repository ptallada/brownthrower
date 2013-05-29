#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import json
import jsonschema
import textwrap
import yaml

from brownthrower import interface, model

try:
    from logging import NullHandler
except ImportError:
    from logutils import NullHandler

log = logging.getLogger('brownthrower.api.task')
log.addHandler(NullHandler())

################################################################################
# PRIVATE                                                                      #
################################################################################

def _validate_ep(entry):
    task = entry.load()
    
    assert isinstance(task.__brownthrower_name__, basestring)
    
    assert len(get_help(task)[0]) > 0
    assert len(get_help(task)[1]) > 0
    
    for dataset in ['config', 'input', 'output']:
        assert isinstance(get_dataset(dataset, 'sample')(task), basestring)
        assert isinstance(get_dataset(dataset, 'schema')(task), basestring)
        get_validator(dataset)(task, get_dataset(dataset, 'sample')(task))
    
    return task

################################################################################
# PUBLIC API                                                                   #
################################################################################

class InvalidStatusException(Exception):
    def __init__(self, message=None):
        self.message = message
        
    def __str__(self):
        return str(self.message)

class UnavailableException(Exception):
    def __init__(self, task=None):
        self.task = task

class ValidationException(Exception):
    def __init__(self, task=None, dataset=None, exception=None):
        self.dataset   = dataset
        self.exception = exception
        self.task      = task
        
    def __str__(self):
        return str(self.exception)

def get_checker(name):
    def check(task, data):
        checker = getattr(task, 'check_%s' % name)
        if checker:
            checker(data)
    return check

def get_dataset(name, attr):
    meth = '%s_%s' % (name, attr)
    def dataset(task):
        return textwrap.dedent(getattr(task, meth)).strip() + '\n'
    return dataset

def get_help(task):
    doc = task.__doc__.strip().split('\n')
    short = doc[0].strip()
    detail = textwrap.dedent('\n'.join(doc[1:])).strip()
    
    return (short, detail)

def get_name(task):
    return task.__brownthrower_name__

def get_validator(name):
    def validate(task, dataset):
        try:
            data = yaml.safe_load(dataset)
            jsonschema.validate(data, json.loads(get_dataset(name, 'schema')(task)))
            get_checker(name)(task, data)
        except Exception as e:
            raise ValidationException(task=get_name(task), dataset=name, exception=e)
    return validate

################################################################################
# TASK OPERATIONS                                                              #
################################################################################

def cancel(job_id):
    session = model.session_maker()
    
    job = session.query(model.Job).filter_by(id = job_id).one()
    
    ancestors = job.ancestors(lockmode='update')[1:]
    
    if job.status not in [
        interface.constants.JobStatus.QUEUED,
        interface.constants.JobStatus.PROCESSING,
        interface.constants.JobStatus.FAILING,
    ]:
        raise InvalidStatusException("This job cannot be cancelled in its current status.")
    
    job.cancel()
    
    for ancestor in ancestors:
        ancestor.update_status()

def create(name):
    """\
    @raise brownthrower.api.task.UnavailableException
    """
    from .. import api
    
    session = model.session_maker()
    
    task = api.get_task(name)
    
    job =  model.Job(
        task    = name,
        config  = get_dataset('config', 'sample')(task),
        status  = interface.constants.JobStatus.STASHED
    )
    
    session.add(job)
    session.flush()
    
    return job

def link(parent_id, child_id):
    session = model.session_maker()
    
    parent = session.query(model.Job).filter_by(
        id = parent_id,
    ).with_lockmode('read').one()
    
    child = session.query(model.Job).filter_by(
        id     = child_id,
    ).with_lockmode('read').one()
    
    if child.status != interface.constants.JobStatus.STASHED:
        raise InvalidStatusException("The child job must be in the stash.")
        
    if parent.super_id or child.super_id:
        raise InvalidStatusException("A parent-child dependency can only be manually established between top-level jobs.")
        
    dependency = model.Dependency(
        child_job_id  = child.id,
        parent_job_id = parent.id
    )
    session.add(dependency)

def remove(job_id):
    session = model.session_maker()
    
    job = session.query(model.Job).filter_by(id = job_id).with_lockmode('update').one()
    
    if job.super_id:
        raise InvalidStatusException("This job is not a top-level job and cannot be removed.")
    
    if job.status not in [
        interface.constants.JobStatus.STASHED,
        interface.constants.JobStatus.CANCELLED,
        interface.constants.JobStatus.FAILED,
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
        interface.constants.JobStatus.CANCELLED,
        interface.constants.JobStatus.FAILED,
        interface.constants.JobStatus.QUEUED,
    ]:
        raise InvalidStatusException("This job cannot be returned to the stash in its current status.")
    
    job.status     = interface.constants.JobStatus.STASHED
    job.ts_queued  = None
    job.ts_started = None
    job.ts_ended   = None

def submit(job_id):
    """\
    @raise brownthrower.api.task.UnavailableException
    """
    
    from .. import api
    
    session = model.session_maker()
    
    job = session.query(model.Job).filter_by(id = job_id).one()
    
    ancestors = job.ancestors(lockmode='update')[1:]
    
    if job.status not in [
        interface.constants.JobStatus.STASHED,
        interface.constants.JobStatus.CANCELLED,
        interface.constants.JobStatus.FAILED,
    ]:
        raise InvalidStatusException("This job cannot be submitted in its current status.")
    
    if job.status == interface.constants.JobStatus.STASHED:
        task = api.get_task(job.task)
        
        get_validator('config')(task, job.config)
        if not job.parents:
            get_validator('input')(task, job.input)
    
    job.submit()
    
    for ancestor in ancestors:
        ancestor.update_status()
