#! /usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import logging
import yaml

from brownthrower import api
from brownthrower import interface
from brownthrower import model
from brownthrower.interface import constants
from contextlib import contextmanager

# TODO: read and create a global or local configuration file
_CONFIG = {
    'entry_points.task'  : 'brownthrower.task',
    'manager.editor'     : 'nano',
    'database.url'       : 'postgresql://tallada:secret,@db01.pau.pic.es/catalogues',
}

log = logging.getLogger('brownthrower.dispatcher.serial')

class SerialDispatcher(interface.Dispatcher):
    """\
    Basic serial dispatcher for testing and development.
    
    This dispatcher executes the jobs one by one in succession.
    It supports both SQLite and PostgreSQL.
    """
    
    def __init__(self, *args, **kwargs):
        api.init(_CONFIG['entry_points.task'])
    
    def _queued_jobs(self):
        while True:
            try:
                # Fetch first job which WAS suitable to be executed
                job = model.session.query(model.Job).filter(
                    model.Job.status == constants.JobStatus.QUEUED,
                    model.Job.task.in_(api.get_tasks().keys()),
                    ~ model.Job.parents.any( #@UndefinedVariable
                        model.Job.status != constants.JobStatus.DONE,
                    )
                ).first()
                
                if not job:
                    #log.info("There are no more jobs suitable to be executed.")
                    break
                
                ancestors = job.ancestors(lockmode='update')[1:]
                
                # Check again after locking if it is still runnable
                if job.status != constants.JobStatus.QUEUED:
                    log.debug("Skipping this job as it has changed its status before being locked.")
                    continue
                
                # Check parents to see if it is still runnable
                parents = model.session.query(model.Job).filter(
                    model.Job.children.contains(job) #@UndefinedVariable
                ).with_lockmode('read').all()
                if filter(lambda parent: parent.status != constants.JobStatus.DONE, parents):
                    log.debug("Skipping this job as some of its parents have changed its status before being locked.")
                    continue
                
                yield (job, ancestors)
            
            except model.NoResultFound:
                log.debug("Skipping this job as it has been removed before being locked.")
            
            finally:
                # Unlock the job if it was skipped
                model.session.rollback()
    
    def _validate_task(self, job):
        if job.parents:
            job.input = yaml.dump(
                [ yaml.safe_load(parent.output) for parent in job.parents ],
                default_flow_style = False
            )
        
        task = api.get_task(job.task)
        api.validate_config(task, job.config)
        api.validate_input(task, job.input)
        
        return task
    
    def _run_prolog(self, job):
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
        subjobs = {}
        task = api.get_task(job.task)(config = yaml.safe_load(job.config))
        
        if hasattr(task, 'prolog'):
            try:
                prolog = task.prolog(tasks=api.get_tasks(), inp=yaml.safe_load(job.input))
                
                for subjob in prolog.get('subjobs', []):
                    subjobs[subjob]  = model.Job(
                            status   = constants.JobStatus.QUEUED,
                            config   = yaml.dump(subjob.config, default_flow_style=False),
                            task     = subjob.name,
                    )
                
                for (subjob, inp) in prolog.get('input', {}).iteritems():
                    subjobs[subjob].input = yaml.dump(inp, default_flow_style=False)
                
                for link in prolog.get('links', []):
                    subjobs[link[0]].children.append(subjobs[link[1]])
            
            except NotImplementedError:
                pass
        
        return subjobs
    
    def _run_epilog(self, job, tasks):
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
        leaf_subjobs = model.session.query(model.Job).filter(
            model.Job.superjob == job,
            ~model.Job.children.any(), #@UndefinedVariable
        ).all()
        out = [yaml.safe_load(subjob.output) for subjob in leaf_subjobs]
        epilog = tasks[job.task](config = yaml.safe_load(job.config)).epilog(tasks=tasks, out=out)
        
        children = {}
        for child in epilog.get('children', []):
            children[child] = model.Job(
                    status  = constants.JobStatus.QUEUED,
                    config  = yaml.dump(child.config, default_flow_style=False),
                    task    = child.name,
            )
        
        for link in epilog.get('links', []):
            children[link[0]].children.append(children[link[1]])
        
        return (children, epilog['output'])
    
    @contextmanager
    def _locked(self, job):
        job = model.session.query(model.Job).filter_by(id = job.id).one()
        ancestors = job.ancestors(lockmode='update')[1:]
        
        if job.status == constants.JobStatus.CANCELLING:
            raise interface.TaskCancelledException()
        
        yield
        
        for ancestor in ancestors:
            ancestor.update_status()
        
    
    def run(self):
        try:
            for (job, ancestors) in self._queued_jobs():
                try:
                    with model.session.begin_nested():
                        log.info("Validating queued job %d of task '%s'." % (job.id, job.task))
                        
                        task = self._validate_task(job)
                        
                        job.status = constants.JobStatus.PROCESSING
                        job.ts_started = datetime.datetime.now()
                        
                        for ancestor in ancestors:
                            ancestor.update_status()
                    
                    # Job is now PROCESSING
                    model.session.commit()
                    
                    with model.session.begin_nested():
                        if not job.subjobs:
                            log.info("Executing prolog of job %d." % job.id)
                            
                            subjobs = self._run_prolog(job)
                            if subjobs:
                                with self._locked(job):
                                    job.subjobs.extend(subjobs.itervalues())
                                
                                continue
                            
                            log.info("Executing job %d." % job.id)
                            
                            runner = interface.Runner(job_id = job.id)
                            out = runner.run(task(config=yaml.safe_load(job.config)), inp=yaml.safe_load(job.input))
                            
                            with self._locked(job):
                                job.output = yaml.safe_dump(out, default_flow_style=False)
                                api.validate_output(task, job.output)
                                job.status = constants.JobStatus.DONE
                                job.ts_ended = datetime.datetime.now()
                            
                        else:
                            log.info("Executing epilog of job %d." % job.id)
                            
                            (children, out) = self._run_epilog(job, api.get_tasks())
                            
                            with self._locked(job):
                                if children:
                                    job.children.append(children.itervalues())
                                
                                job.output = yaml.dump(out, default_flow_style=False)
                                api.validate_output(task, job.output)
                                job.status = constants.JobStatus.DONE
                                job.ts_ended = datetime.datetime.now()
                
                except BaseException as e:
                    try:
                        raise
                    except interface.TaskCancelledException:
                        job.status = constants.JobStatus.CANCELLED
                    except Exception:
                        job.status = constants.JobStatus.FAILED
                    except BaseException:
                        job.status = constants.JobStatus.CANCELLED
                        raise
                    finally:
                        job = model.session.query(model.Job).filter_by(id = job.id).one()
                        
                        ancestors = job.ancestors(lockmode='update')[1:]
                        
                        for ancestor in ancestors:
                            ancestor.update_status()
                        
                        job.ts_ended = datetime.datetime.now()
                        # Set start time in case the job fail to validate
                        if not job.ts_started:
                            job.ts_started = job.ts_ended
                        
                        log.error("Execution of job %d ended with status '%s'." % (job.id, job.status))
                        log.debug(e)
                
                finally:
                    model.session.commit()
            
            log.info("No more jobs to run.")
        
        finally:
            model.session.rollback()

def main():
    import time
    import signal
    import sys
    
    def system_exit(*args, **kwargs):
        sys.exit(1)
    
    signal.signal(signal.SIGTERM, system_exit)
    
    # TODO: Remove
    #logging.basicConfig(level=logging.DEBUG)
    #logging.getLogger('sqlalchemy.engine').setLevel(logging.DEBUG)
    
    #from pysrc import pydevd
    #pydevd.settrace(suspend=False)
    
    #import rpdb
    #rpdb.Rpdb().set_trace()
    
    url = _CONFIG['database.url']
    #url = 'sqlite:////tmp/manager.db'
    model.init(url)
    model.Base.metadata.create_all() #@UndefinedVariable
    
    dispatcher = SerialDispatcher()
    while True:
        dispatcher.run()
        time.sleep(60)
    
if __name__ == '__main__':
    main()