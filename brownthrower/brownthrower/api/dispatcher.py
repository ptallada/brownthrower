#! /usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import logging
import yaml

from brownthrower import interface, model
from contextlib import contextmanager
from itertools import imap
from sqlalchemy.orm.exc import NoResultFound

try:
    from logging import NullHandler
except ImportError:
    from logutils import NullHandler # @UnusedImport

log = logging.getLogger('brownthrower.api.dispatcher')
log.addHandler(NullHandler())

class NoRunnableJobFound(Exception):
    pass

class RequiredJobNotRunnable(Exception):
    pass

def _validate_task(job):
    from .. import api
    
    if job.parents:
        job.input = yaml.safe_dump(
            [ yaml.safe_load(parent.output) for parent in job.parents ],
            default_flow_style = False
        )
    
    task = api.get_task(job.task)
    api.task.get_validator('config')(task, job.config)
    api.task.get_validator('input' )(task, job.input)
    
    return task(config = yaml.safe_load(job.config))

def get_runnable_job(job_id=None):
    """
    @raise NoResultFound: No job_id was specified and no runnable job could be found.
    @raise RequiredJobNotRunnable: The specified job_id could not be found or is not runnable.
    """
    from .. import api
    
    while True:
        session = model.session_maker()
        
        # Fetch first job which WAS suitable to be executed
        job = session.query(model.Job).filter(
            model.Job.status == interface.constants.JobStatus.QUEUED,
            model.Job.task.in_(api.get_tasks().keys()),
            ~ model.Job.parents.any( # @UndefinedVariable
                model.Job.status != interface.constants.JobStatus.DONE,
            )
        )
        if job_id:
            try:
                job = job.filter_by(id = job_id).one()
            except NoResultFound:
                raise RequiredJobNotRunnable
        else:
            job = job.first()
            if not job:
                raise NoRunnableJobFound
        
        try:
            # Lock job and recheck if it is still runnable
            ancestors = job.ancestors(lockmode='update')[1:]
            
            assert job.status == interface.constants.JobStatus.QUEUED
            
            parents = session.query(model.Job).filter(
                model.Job.children.contains(job) # @UndefinedVariable
            ).with_lockmode('read').all()
            assert all(imap(lambda parent: parent.status == interface.constants.JobStatus.DONE, parents))
            
            return (job, ancestors)
        
        except (NoResultFound, AssertionError):
            session.rollback()

def process_job(job, ancestors):
    task = _validate_task(job)
    
    job.status = interface.constants.JobStatus.PROCESSING
    job.ts_started = datetime.datetime.now()
    
    for ancestor in ancestors:
        ancestor.update_status()
    
    return task

def preload_job(job):
    session = model.session_maker()
    
    #session.refresh(job)
    assert len(job.subjobs) >= 0
    job._leaf_subjobs = session.query(model.Job).filter(
        model.Job.superjob == job,
        ~model.Job.children.any(), # @UndefinedVariable
    ).all()
    
    session.flush()
    session.expunge(job)
    
    return job

def _run_prolog(task, inp):
    """
    {
        'subjobs' : [
            Task_A(config),
            Task_B(config),
            Task_B(config),
        ],
        'input' : {
            task_M : <input>,
            task_N : <input>,
        }
        'links' : [
            ( task_X, task_Y ),
        ]
    }
    """
    from .. import api
    
    subjobs = {}
    
    if hasattr(task, 'prolog'):
        try:
            prolog = task.prolog(tasks=api.get_tasks(), inp=yaml.safe_load(inp))
        except NotImplementedError:
            pass
        else:
            for subjob in prolog.get('subjobs', []):
                subjobs[subjob]  = model.Job(
                        status   = interface.constants.JobStatus.QUEUED,
                        config   = yaml.safe_dump(subjob.config, default_flow_style=False),
                        task     = api.task.get_name(subjob),
                )
            
            for (subjob, inp) in prolog.get('input', {}).iteritems():
                subjobs[subjob].input = yaml.safe_dump(inp, default_flow_style=False)
            
            for link in prolog.get('links', []):
                subjobs[link[0]].children.append(subjobs[link[1]])
    
    return subjobs

def _run_epilog(task, leaf_subjobs):
    """
    {
        'children' : [
            Task_A(config),
            Task_B(config),
            Task_B(config),
        ],
        'links' : [
            ( task_X, task_Y ),
        ]
        'output' : <output>
    }
    """
    
    from .. import api
    
    out = [yaml.safe_load(subjob.output) for subjob in leaf_subjobs]
    epilog = task.epilog(tasks=api.get_tasks(), out=out)
    
    children = {}
    for child in epilog.get('children', []):
        children[child] = model.Job(
                status  = interface.constants.JobStatus.QUEUED,
                config  = yaml.safe_dump(child.config, default_flow_style=False),
                task    = api.task.get_name(child),
        )
    
    for link in epilog.get('links', []):
        children[link[0]].children.append(children[link[1]])
    
    return (children, epilog['output'])

@contextmanager
def _locked(job_id):
    session = model.session_maker()
    
    job = session.query(model.Job).filter_by(id = job_id).one()
    ancestors = job.ancestors(lockmode='update')[1:]
    
    if job.status == interface.constants.JobStatus.CANCELLING:
        raise interface.task.CancelledException()
    
    yield job
    
    for ancestor in ancestors:
        ancestor.update_status()

def run_job(preloaded_job, task):
    """
    Requires a detached Job instance with the following attributes loaded:
      subjobs : Job instances in which this job has decomposed into
      _leaf_subjobs : Job instances in which this job has decomposed into and that do not have any other Job depending on them.
    """
    
    from .. import api
    
    if not preloaded_job.subjobs:
        log.debug("Executing prolog of job %d." % preloaded_job.id)
        
        subjobs = _run_prolog(task, preloaded_job.input)
        if subjobs:
            with _locked(preloaded_job.id) as job:
                job.subjobs.extend(subjobs.itervalues())
            
            return
        
        log.debug("Executing job %d." % preloaded_job.id)
        
        runner = interface.runner.Runner(job_id = preloaded_job.id)
        out = runner.run(task, inp=yaml.safe_load(preloaded_job.input))
        
        with _locked(preloaded_job.id) as job:
            job.output = yaml.safe_dump(out, default_flow_style=False)
            api.task.get_validator('output')(task, job.output)
            job.status = interface.constants.JobStatus.DONE
            job.ts_ended = datetime.datetime.now()
    
    else:
        log.debug("Executing epilog of job %d." % preloaded_job.id)
        
        (children, out) = _run_epilog(task, preloaded_job._leaf_subjobs)
        
        with _locked(preloaded_job.id) as job:
            if children:
                job.children.append(children.itervalues())
            
            job.output = yaml.safe_dump(out, default_flow_style=False)
            api.task.get_validator('output')(task, job.output)
            job.status = interface.constants.JobStatus.DONE
            job.ts_ended = datetime.datetime.now()

def handle_job_exception(preloaded_job, e):
    try:
        raise e
    except interface.task.CancelledException:
        preloaded_job.status = interface.constants.JobStatus.CANCELLED
    except Exception:
        preloaded_job.status = interface.constants.JobStatus.FAILED
    except BaseException:
        preloaded_job.status = interface.constants.JobStatus.CANCELLED
        raise
    finally:
        session = model.session_maker()
        job = session.query(model.Job).filter_by(id = preloaded_job.id).one()
        ancestors = job.ancestors(lockmode='update')[1:]
        
        job.status = preloaded_job.status
        for ancestor in ancestors:
            ancestor.update_status()
        
        job.ts_ended = datetime.datetime.now()
        # Set start time in case the job fail to validate
        if not job.ts_started:
            job.ts_started = job.ts_ended
