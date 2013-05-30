#! /usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import datetime
import logging
import sys
import textwrap
import time
import transaction
import yaml

from brownthrower import api, interface, model, release
from contextlib import contextmanager
from sqlalchemy.orm.exc import NoResultFound

try:
    from logging import NullHandler
except ImportError:
    from logutils import NullHandler # @UnusedImport

log = logging.getLogger('brownthrower.dispatcher.serial')
log.addHandler(NullHandler())

class SerialDispatcher(interface.dispatcher.Dispatcher):
    """\
    Basic serial dispatcher for testing and development.
    
    This dispatcher executes the jobs one by one in succession.
    It supports both SQLite and PostgreSQL.
    """
    
    __brownthrower_name__ = 'serial'
    
    def usage(self):
        print textwrap.dedent("""\
        usage: dispatcher run serial [id]
        
        Optionally, a single job id can be provided to restrict the job that may be
        run by this dispatcher.""".format(
            name = self.__brownthrower_name__
        ))
    
    def check_usage(self, *args):
        assert len(args) < 2
        if len(args) == 1:
            assert int(args[0])
    
    def _get_runnable_job(self, job_id = None):
        """
        @raise NoResultFound: If job_id is provided and cannot be run.
        """
        while True:
            session = model.session_maker()
            
            # Fetch first job which WAS suitable to be executed
            job = session.query(model.Job).filter(
                model.Job.status == interface.constants.JobStatus.QUEUED,
                model.Job.task.in_(api.get_tasks().keys()),
                ~ model.Job.parents.any(
                    model.Job.status != interface.constants.JobStatus.DONE,
                )
            )
            
            if job_id:
                # Require that the given job id exists and it is runnable.
                job = job.filter_by(id = job_id).one()
            else:
                job = job.first()
            
            if not job:
                # There are no more jobs suitable to be executed
                transaction.abort()
                return (None, None)
            
            try:
                ancestors = job.ancestors(lockmode='update')[1:]
            except NoResultFound:
                # Skipping this job as it has been removed before being locked
                transaction.abort()
                continue
            
            # Check again after locking if it is still runnable
            if job.status != interface.constants.JobStatus.QUEUED:
                # Skipping this job as it has changed its status before being locked
                transaction.abort()
                continue
            
            # Check parents to see if it is still runnable
            parents = session.query(model.Job).filter(
                model.Job.children.contains(job)
            ).with_lockmode('read').all()
            if filter(lambda parent: parent.status != interface.constants.JobStatus.DONE, parents):
                # Skipping this job as some of its parents have changed its status before being locked
                transaction.abort()
                continue
            
            return (job, ancestors)
    
    def _validate_task(self, job):
        if job.parents:
            job.input = yaml.safe_dump(
                [ yaml.safe_load(parent.output) for parent in job.parents ],
                default_flow_style = False
            )
        
        task = api.get_task(job.task)
        api.task.get_validator('config')(task, job.config)
        api.task.get_validator('input' )(task, job.input)
        
        return task(config = yaml.safe_load(job.config))
    
    def _run_prolog(self, task, inp):
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
    
    def _run_epilog(self, task, job, leaf_subjobs):
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
    def _locked(self, job_id):
        session = model.session_maker()
        
        job = session.query(model.Job).filter_by(id = job_id).one()
        ancestors = job.ancestors(lockmode='update')[1:]
        
        if job.status == interface.constants.JobStatus.CANCELLING:
            raise interface.task.CancelledException()
        
        yield job
        
        for ancestor in ancestors:
            ancestor.update_status()
    
    def _run_job(self, job, ancestors):
        try:
            session = model.session_maker()
            
            log.info("Validating queued job %d of task '%s'." % (job.id, job.task))
            
            task = self._validate_task(job)
            
            job.status = interface.constants.JobStatus.PROCESSING
            job.ts_started = datetime.datetime.now()
            
            for ancestor in ancestors:
                ancestor.update_status()
            session.flush()
            
            # Preload subjobs for the next steps
            assert len(job.subjobs) >= 0
            leaf_subjobs = session.query(model.Job).filter(
                model.Job.superjob == job,
                ~model.Job.children.any(),
            ).all()
            
            # Job is now PROCESSING
            session.flush()
            session.expunge(job)
            transaction.commit()
            
            session = model.session_maker()
            with transaction.manager:
                if not job.subjobs:
                    log.info("Executing prolog of job %d." % job.id)
                    
                    subjobs = self._run_prolog(task, job.input)
                    if subjobs:
                        with self._locked(job.id) as job:
                            job.subjobs.extend(subjobs.itervalues())
                        
                        return
                    
                    log.info("Executing job %d." % job.id)
                    
                    runner = interface.runner.Runner(job_id = job.id)
                    out = runner.run(task, inp=yaml.safe_load(job.input))
                    
                    with self._locked(job.id) as job:
                        job.output = yaml.safe_dump(out, default_flow_style=False)
                        api.task.get_validator('output')(task, job.output)
                        job.status = interface.constants.JobStatus.DONE
                        job.ts_ended = datetime.datetime.now()
                    
                else:
                    log.info("Executing epilog of job %d." % job.id)
                    
                    (children, out) = self._run_epilog(task, job, leaf_subjobs)
                    
                    with self._locked(job.id) as job:
                        if children:
                            job.children.append(children.itervalues())
                        
                        job.output = yaml.safe_dump(out, default_flow_style=False)
                        api.task.get_validator('output')(task, job.output)
                        job.status = interface.constants.JobStatus.DONE
                        job.ts_ended = datetime.datetime.now()
        
        except BaseException as e:
            session = model.session_maker()
            try:
                job = session.query(model.Job).filter_by(id = job.id).one()
                ancestors = job.ancestors(lockmode='update')[1:]
                raise
            except interface.task.CancelledException:
                job.status = interface.constants.JobStatus.CANCELLED
            except Exception:
                job.status = interface.constants.JobStatus.FAILED
            except BaseException:
                job.status = interface.constants.JobStatus.CANCELLED
                raise
            finally:
                for ancestor in ancestors:
                    ancestor.update_status()
                
                job.ts_ended = datetime.datetime.now()
                # Set start time in case the job fail to validate
                if not job.ts_started:
                    job.ts_started = job.ts_ended
                
                log.error("Execution of job %d ended with status '%s'." % (job.id, job.status))
                log.debug(e)
        
        finally:
            transaction.commit()
    
    def _run(self, job_id):
        if job_id:
            try:
                (job, ancestors) = self._get_runnable_job(job_id)
                
                self._run_job(job, ancestors)
                
            except NoResultFound:
                raise Exception("The job with id %s cannot be found or it is not runnable." % job_id)
        else:
            while True:
                (job, ancestors) = self._get_runnable_job()
                
                if not job:
                    # No more jobs to run
                    break
                
                self._run_job(job, ancestors)
            
            log.info("No more jobs to run.")
    
    def run(self, *args):
        try:
            self.check_usage(*args)
        except Exception:
            return self.usage()
        
        job_id = None
        if len(args) == 1:
            job_id = args[0]
        
        self._run(job_id)

def _parse_args(args = None):
    parser = argparse.ArgumentParser(prog='manager')
    parser.add_argument('job_id', type=int, nargs='?', default=argparse.SUPPRESS,
                        help="run this specific job")
    parser.add_argument('-p', '--profile', const='default', nargs='?', default='default',
                        help="configuration profile for this session (default: 'default')")
    parser.add_argument('-u', '--database-url', default=argparse.SUPPRESS,
                        help='database connection settings')
    parser.add_argument('-d', '--debug', const='pdb', nargs='?', default=argparse.SUPPRESS,
                        help="enable debugging framework (deactivated by default, 'pdb' if framework is not specified)",
                        choices=['pydevd', 'ipdb', 'rpdb', 'pdb'])
    parser.add_argument('-v', '--version', action='version', 
                        version='%%(prog)s %s' % release.__version__)
    
    options = vars(parser.parse_args(args))
    
    return options

def main(args = None):
    if not args:
        args = sys.argv[1:]
    
    dispatcher = SerialDispatcher()
    print "brownthrower dispatcher serial v{version} is loading...".format(
        version = release.__version__
    )
    options = _parse_args(args)
    job_id = options.get('job_id', None)
    api.init(options)
    
    try:
        dispatcher._run(job_id)
        if not job_id:
            while True:
                dispatcher._run()
                time.sleep(60)
    
    except KeyboardInterrupt:
        pass

if __name__ == '__main__':
    main()
