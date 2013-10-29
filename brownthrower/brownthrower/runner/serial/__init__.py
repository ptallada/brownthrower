#! /usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import datetime
import email.message
import logging
import os
import repoze.sendmail.delivery
import repoze.sendmail.mailer
import shutil
import sys
import textwrap
import time
import traceback
import transaction
import yaml

from brownthrower import api, interface, model, release
from brownthrower.api.profile import settings
from contextlib import contextmanager
from itertools import imap
from sqlalchemy.orm.exc import NoResultFound

try:
    from logging import NullHandler
except ImportError:
    from logutils import NullHandler # @UnusedImport

log = logging.getLogger('brownthrower.runner.serial')
log.addHandler(NullHandler())


class NoRunnableJobFound(Exception):
    pass

class RequiredJobNotRunnable(Exception):
    pass

class SerialRunner(object):
    """\
    Basic serial runner for testing and development.
    
    This runner executes the jobs one by one in succession.
    It supports both SQLite and PostgreSQL.
    """
    
    __brownthrower_name__ = 'serial'
    
    def _parse_args(self, args):
        parser = argparse.ArgumentParser(prog='runner.serial', add_help=False)
        parser.add_argument('--archive-logs', default=argparse.SUPPRESS, metavar='PATH',
                            help='store a copy of each job log in %(metavar)s')
        parser.add_argument('--database-url', '-u', default=argparse.SUPPRESS, metavar='URL',
                            help='use the settings in %(metavar)s to establish the database connection')
        parser.add_argument('--debug', '-d', const='pdb', nargs='?', default=argparse.SUPPRESS,
                            help="enable debugging framework (deactivated by default, '%(const)s' if no specific framework is requested)",
                            choices=['pydevd', 'ipdb', 'rpdb', 'pdb'])
        parser.add_argument('--help', '-h', action='help',
                            help='show this help message and exit')
        group = parser.add_mutually_exclusive_group()
        group.add_argument('--job-id', '-j', type=int, default=argparse.SUPPRESS, metavar='ID',
                            help="run only the job identified by %(metavar)s")
        group.add_argument('--loop', metavar='NUMBER', nargs='?', type=int, const=60, default=argparse.SUPPRESS,
                            help="enable infinite looping, waiting %(metavar)s seconds between iterations (default: %(const)s)")
        parser.add_argument('--notify-failed', default=argparse.SUPPRESS, metavar='EMAIL',
                            help='report failed jobs to this address')
        parser.add_argument('--post-mortem', const='pdb', nargs='?', default=argparse.SUPPRESS,
                            help="enable post-mortem debugging (deactivated by default, '%(const)s' if no specific framework is requested)",
                            choices=['ipdb', 'pdb'])
        parser.add_argument('--profile', '-p', default='default', metavar='NAME',
                            help="load the profile %(metavar)s at startup (default: '%(default)s')")
        parser.add_argument('--version', '-v', action='version', 
                            version='%%(prog)s %s' % release.__version__)
        
        options = vars(parser.parse_args(args))
        
        return options
    
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
    
    def get_runnable_job(self, job_id=None):
        """
        @raise NoResultFound: No job_id was specified and no runnable job could be found.
        @raise RequiredJobNotRunnable: The specified job_id could not be found or is not runnable.
        """
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
                transaction.abort()
    
    def process_job(self, job, ancestors):
        session = model.session_maker()
        
        task = self._validate_task(job)
        
        job.status = interface.constants.JobStatus.PROCESSING
        job.ts_started = datetime.datetime.now()
        
        for ancestor in ancestors:
            ancestor.update_status()
        
        session.flush()
        session.expunge(job)
        
        return task
    
    def preload_job(self, job):
        session = model.session_maker()
        
        assert len(job.subjobs) >= 0
        job._leaf_subjobs = session.query(model.Job).filter(
            model.Job.superjob == job,
            ~model.Job.children.any(), # @UndefinedVariable
        ).all()
        
        return job
    
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
    
    def _run_epilog(self, task, leaf_subjobs):
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
    
    def run_job(self, preloaded_job, task):
        """
        Requires a detached Job instance with the following attributes loaded:
          subjobs : Job instances in which this job has decomposed into
          _leaf_subjobs : Job instances in which this job has decomposed into and that do not have any other Job depending on them.
        """
        
        if not preloaded_job.subjobs:
            log.debug("Executing prolog of job %d." % preloaded_job.id)
            
            subjobs = self._run_prolog(task, preloaded_job.input)
            if subjobs:
                with self._locked(preloaded_job.id) as job:
                    job.subjobs.extend(subjobs.itervalues())
                
                return
            
            log.debug("Executing job %d." % preloaded_job.id)
            
            context = interface.context.Context(job_id = preloaded_job.id)
            out = context.run(task, inp=yaml.safe_load(preloaded_job.input))
            
            with self._locked(preloaded_job.id) as job:
                job.output = yaml.safe_dump(out, default_flow_style=False)
                api.task.get_validator('output')(task, job.output)
                job.status = interface.constants.JobStatus.DONE
                job.ts_ended = datetime.datetime.now()
        
        else:
            log.debug("Executing epilog of job %d." % preloaded_job.id)
            
            (children, out) = self._run_epilog(task, preloaded_job._leaf_subjobs)
            
            with self._locked(preloaded_job.id) as job:
                if children:
                    job.children.append(children.itervalues())
                
                job.output = yaml.safe_dump(out, default_flow_style=False)
                api.task.get_validator('output')(task, job.output)
                job.status = interface.constants.JobStatus.DONE
                job.ts_ended = datetime.datetime.now()
    
    def handle_job_exception(self, preloaded_job, e):
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
    
    def _notify_failed(self, address, job, tb):
        # TODO: Move to configuration
        mailer = repoze.sendmail.mailer.SMTPMailer('relay.pic.es')
        delivery = repoze.sendmail.delivery.DirectMailDelivery(mailer)
        
        message = email.message.Message()
        message['From'] = 'brownthrower.dispatcher.static <operador@pic.es>'
        message['To'] = '<%s>' % address
        message['Subject'] = "The job %d of task '%s' has FAILED" % (job.id, job.task)
        message.set_payload(textwrap.dedent("""\
            Job {id} of task {task} has aborted with status FAILED.
            
            Input
            -----
            {input}
            
            Config
            ------
            {config}
            
            Traceback
            ---------
            {traceback}
            """).format(
                id        = job.id,
                task      = job.task,
                input     = job.input,
                config    = job.config,
                traceback = tb,
            )
        )
        
        delivery.send('operador@pic.es', address, message)
    
    def _enter_postmortem(self, module):
        dbg = None
        if module == 'pdb':
            import pdb
            dbg = pdb
        elif module == 'ipdb':
            import ipdb
            dbg = ipdb
        
        tb = sys.exc_info()[2]
        dbg.post_mortem(tb)
    
    def _run_job(self, post_mortem = None, job_id = None, notify_failed = None, archive_logs = None):
        try:
            (job, ancestors) = self.get_runnable_job(job_id)
            preloaded_job = self.preload_job(job)
            log.info("Job %d has been locked and it is being processed." % job.id)
        except BaseException:
            transaction.abort()
            raise
        
        try:
            try:
                task = self.process_job(preloaded_job, ancestors)
                transaction.commit()
            except BaseException:
                transaction.abort()
                raise
            
            log.info("Job %d is now in PROCESSING state and it is being run." % preloaded_job.id)
            
            with transaction.manager:
                self.run_job(preloaded_job, task)
            
            log.info("Job %d has finished successfully." % preloaded_job.id)
        
        except BaseException as e:
            tb = traceback.format_exc()
            log.debug(tb)
            
            try:
                self.handle_job_exception(preloaded_job, e)
            finally:
                transaction.commit()
                log.warning("Job %d was aborted with status '%s'." % (preloaded_job.id, preloaded_job.status))
                
            if preloaded_job.status == interface.constants.JobStatus.FAILED:
                if notify_failed:
                    with transaction.manager:
                        self._notify_failed(notify_failed, preloaded_job, tb)
                
                if post_mortem:
                    self._enter_postmortem(post_mortem)
        
        finally:
            if archive_logs:
                self._archive_log(archive_logs, preloaded_job)
    
    def _setup_log_archiving(self):
        self.root_logger = logging.getLogger()
        self.handler = logging.handlers.WatchedFileHandler('job.log')
        self.root_logger.addHandler(self.handler)
    
    def _archive_log(self, archive_logs, preloaded_job):
        # TODO: use template for naming
        shutil.copy('job.log', os.path.join(archive_logs, '%s_%d.log' % (preloaded_job.task, preloaded_job.id)))
        os.unlink('job.log')
    
    def run(self, args = []):
        options = self._parse_args(args)
        
        job_id = options.pop('job_id', None)
        loop = options.pop('loop', 0)
        post_mortem = options.pop('post_mortem', None)
        notify_failed = options.pop('notify_failed', None)
        archive_logs  = options.pop('archive_logs', None)
        
        api.init(options)
        
        try:
            if archive_logs:
                self._setup_log_archiving()
            
            if job_id:
                self._run_job(post_mortem, job_id, notify_failed = notify_failed, archive_logs = archive_logs)
                return
            
            while True:
                try:
                    while True:
                        self._run_job(post_mortem, notify_failed = notify_failed, archive_logs = archive_logs)
                except NoRunnableJobFound:
                    pass
                
                if not loop:
                    return
                
                log.info("No runnable jobs found. Sleeping %d seconds until next iteration." % loop)
                time.sleep(loop)
        
        except KeyboardInterrupt:
            pass

def main(args = None):
    if not args:
        args = sys.argv[1:]
    
    print "brownthrower runner serial v{version} is loading...".format(
        version = release.__version__
    )
    runner = SerialRunner()
    runner.run(args)

if __name__ == '__main__':
    main()
