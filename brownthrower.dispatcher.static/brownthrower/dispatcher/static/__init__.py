#! /usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import contextlib
import email.message
import glite.ce.job
import logging
import pkg_resources
import repoze.sendmail.delivery
import repoze.sendmail.mailer
import string
import sys
import tempfile
import textwrap
import traceback
import transaction
import time

from . import release
from brownthrower import api, interface
from brownthrower.api.profile import settings

try:
    from logging import NullHandler
except ImportError:
    from logutils import NullHandler # @UnusedImport

log = logging.getLogger('brownthrower.dispatcher.static')
log.addHandler(NullHandler())

JDL_TEMPLATE = """\
[
Type = "Job";
JobType = "Normal";
Executable = "${executable}";
Arguments = "${arguments}";
StdOutput = "std.out";
StdError = "std.err";
OutputSandbox = {"std.out", "std.err"}; 
OutputSandboxBaseDestUri="gsiftp://localhost";
requirements = other.GlueCEStateStatus == "Production";
rank = -other.GlueCEStateEstimatedResponseTime;
RetryCount = 0;
]
"""

class StaticDispatcher(interface.dispatcher.Dispatcher):
    """\
    TODO
    
    TODO
    TODO
    """
    
    __brownthrower_name__ = 'static'
    
    def _get_arg_parser(self):
        parser = argparse.ArgumentParser(prog='dispatcher.static', add_help=False)
        parser.add_argument('--database-url', '-u', default=argparse.SUPPRESS, metavar='URL',
                            help='use the settings in %(metavar)s to establish the database connection')
        parser.add_argument('--debug', '-d', const='pdb', nargs='?', default=argparse.SUPPRESS,
                            help="enable debugging framework (deactivated by default, '%(const)s' if no specific framework is requested)",
                            choices=['pydevd', 'ipdb', 'rpdb', 'pdb'])
        parser.add_argument('--help', '-h', action='help',
                            help='show this help message and exit')
        parser.add_argument('--mode', '-m', default='dispatch',
                            help="select the mode of operation (default: '%(default)s')",
                            choices=['dispatch', 'run'])
        parser.add_argument('--notify-failed', default=argparse.SUPPRESS, metavar='EMAIL',
                            help='report failed jobs to this address')
        parser.add_argument('--profile', '-p', default='default', metavar='NAME',
                            help="load the profile %(metavar)s at startup (default: '%(default)s')")
        parser.add_argument('--version', '-v', action='version', 
                            version='%%(prog)s %s' % release.__version__)
        
        dispatch = parser.add_argument_group("options for 'dispatch' mode")
        dispatch.add_argument('--ce-queue', metavar='ENDPOINT', default='ce02-test.pic.es:8443/cream-pbs-test_sl6',
                              help="select the batch queue to sent the pilots into")
        dispatch.add_argument('--pool-size', metavar='NUMBER', type=int, default=5,
                              help="set the pool size to %(metavar)s pilots (default: %(default)s)")
        dispatch.add_argument('--remote-path', metavar='COMMAND', default=argparse.SUPPRESS,
                              help="specify the location of this dispatcher in the remote nodes")
        
        run = parser.add_argument_group("options for 'run' mode")
        run.add_argument('--job-id', '-j', type=int, default=argparse.SUPPRESS, metavar='ID',
                         help="run only the job identified by %(metavar)s")
        run.add_argument('--loop', metavar='NUMBER', nargs='?', type=int, const=60, default=argparse.SUPPRESS,
                         help="enable infinite looping, waiting %(metavar)s seconds between iterations (default: %(const)s)")
        run.add_argument('--post-mortem', const='pdb', nargs='?', default=argparse.SUPPRESS,
                         help="enable post-mortem debugging (deactivated by default, '%(const)s' if no specific framework is requested)",
                         choices=['ipdb', 'pdb'])
        
        return parser
    
    @contextlib.contextmanager
    def _write_jdl(self, executable, arguments):
        template = string.Template(JDL_TEMPLATE)
        
        with tempfile.NamedTemporaryFile("w+") as fh:
            fh.write(template.substitute({
                'executable' : executable,
                'arguments' : arguments,
            }))
            
            fh.flush()
            
            yield fh.name
    
    def _dispatch(self, pool_size, remote_path, ce_queue, notify_failed = None):
        final_status = set([
            'ABORTED',
            'CANCELLED',
            'DONE-OK',
            'DONE-FAILED',
        ])
        
        options = [
            '-u', settings['database_url'],
            '--mode', 'run',
            '--loop', '60'
        ]
        
        if notify_failed:
            options.extend(['--notify-failed', notify_failed])
        
        arguments = ' '.join(options)
        
        job_ids = []
        
        try:
            for _ in range(pool_size):
                with self._write_jdl(remote_path, arguments) as jdl_file:
                    job_ids.append(glite.ce.job.submit(jdl_file, endpoint=ce_queue))
                    log.info("Launched a new job with id %s." % job_ids[-1])
            
            while True:
                for jobid in job_ids:
                    status = glite.ce.job.status(endpoint=ce_queue.split('/')[0], jobid=jobid)
                    log.info("Job %s is in status %s." % (jobid, status.attrs['Status']))
                    
                    if status.attrs['Status'] in final_status:
                        job_ids.remove(jobid)
                        with self._write_jdl(remote_path, arguments) as jdl_file:
                            job_ids.append(glite.ce.job.submit(jdl_file, endpoint=ce_queue))
                            log.info("Launched a new job with id %s." % job_ids[-1])
                
                time.sleep(10)
            
        finally:
            log.warning("Shutting down pool. DO NOT INTERRUPT.")
            for jobid in job_ids:
                log.info("Cancelling job %s" % jobid)
                try:
                    glite.ce.job.cancel(jobid)
                except Exception:
                    pass
            
            while job_ids:
                my_jobids = job_ids[:]
                for jobid in my_jobids:
                    status = glite.ce.job.status(endpoint=ce_queue.split('/')[0], jobid=jobid)
                    log.info("Job %s is in status %s." % (jobid, status.attrs['Status']))
                    
                    if status.attrs['Status'] in final_status:
                        job_ids.remove(jobid)
                
                if not job_ids:
                    break
                
                for jobid in job_ids:
                    log.info("Cancelling job %s" % jobid)
                    try:
                        glite.ce.job.cancel(jobid)
                    except Exception:
                        pass
                
                time.sleep(5)
    
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
    
    def _run_job(self, post_mortem = None, job_id = None, notify_failed = None):
        try:
            (job, ancestors) = api.dispatcher.get_runnable_job(job_id)
            preloaded_job = api.dispatcher.preload_job(job)
            log.info("Job %d has been locked and it is being processed." % job.id)
        except BaseException:
            transaction.abort()
            raise
        
        try:
            try:
                task = api.dispatcher.process_job(job, ancestors)
                transaction.commit()
            except BaseException:
                transaction.abort()
                raise
            
            log.info("Job %d is now in PROCESSING state and it is being run." % preloaded_job.id)
            
            with transaction.manager:
                api.dispatcher.run_job(preloaded_job, task)
            
            log.info("Job %d has finished successfully and it is now in DONE state." % preloaded_job.id)
        
        except BaseException as e:
            tb = traceback.format_exc()
            log.debug(tb)
            
            try:
                api.dispatcher.handle_job_exception(preloaded_job, e)
            finally:
                transaction.commit()
                log.warning("Job %d was aborted with status '%s'." % (preloaded_job.id, preloaded_job.status))
            
            if preloaded_job.status == interface.constants.JobStatus.FAILED:
                if notify_failed:
                    with transaction.manager:
                        self._notify_failed(notify_failed, preloaded_job, tb)
                
                if post_mortem:
                    self._enter_postmortem(post_mortem)
    
    def _run(self, job_id, loop, post_mortem, notify_failed):
        if job_id:
            self._run_job(post_mortem, job_id, notify_failed = notify_failed)
            return
        
        while True:
            try:
                while True:
                    self._run_job(post_mortem, notify_failed = notify_failed)
            except api.dispatcher.NoRunnableJobFound:
                pass
            
            if not loop:
                return
            
            log.info("No runnable jobs found. Sleeping %d seconds until next iteration." % loop)
            time.sleep(loop)
    
    def main(self, args = []):
        if not args:
            args = sys.argv[1:]
        
        parser = self._get_arg_parser()
        options = vars(parser.parse_args(args))
        
        # General
        mode          = options.pop('mode')
        notify_failed = options.pop('notify_failed', None)
        # Dispatch mode
        ce_queue    = options.pop('ce_queue', None)
        pool_size   = options.pop('pool_size', None)
        remote_path = options.pop('remote_path', None)
        # Run mode
        job_id      = options.pop('job_id', None)
        loop        = options.pop('loop', 0)
        post_mortem = options.pop('post_mortem', None)
        
        if mode == 'dispatch' and not remote_path:
            parser.error("--remote-path is required in 'dispatch' mode.")
            sys.exit(1)
        
        print "brownthrower dispatcher static v{version} is loading...".format(
            version = release.__version__
        )
        
        api.init(options)
        
        try:
            if mode == 'dispatch':
                self._dispatch(pool_size, remote_path, ce_queue, notify_failed)
            else:
                self._run(job_id, loop, post_mortem, notify_failed)
        except KeyboardInterrupt:
            pass
        
def main(args = []):
    dispatcher = StaticDispatcher()
    dispatcher.main(args)

if __name__ == '__main__':
    main()
