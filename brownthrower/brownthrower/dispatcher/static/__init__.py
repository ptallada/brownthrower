#! /usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import collections
import contextlib
import glite.ce.job
import logging
import string
import sys
import tempfile
import time

from brownthrower import api, release
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

class StaticDispatcher(object):
    """\
    TODO
    
    TODO
    TODO
    """
    
    __brownthrower_name__ = 'static'
    
    def _get_arg_parser(self):
        parser = argparse.ArgumentParser(prog='dispatcher.static', add_help=False)
        parser.add_argument('--ce-queue', metavar='ENDPOINT', default=argparse.SUPPRESS,
                            help="select the batch queue to sent the pilots into", required=True)
        parser.add_argument('--database-url', '-u', default=argparse.SUPPRESS, metavar='URL',
                            help='use the settings in %(metavar)s to establish the database connection')
        parser.add_argument('--debug', '-d', const='pdb', nargs='?', default=argparse.SUPPRESS,
                            help="enable debugging framework (deactivated by default, '%(const)s' if no specific framework is requested)",
                            choices=['pydevd', 'ipdb', 'rpdb', 'pdb'])
        parser.add_argument('--help', '-h', action='help',
                            help='show this help message and exit')
        parser.add_argument('--pool-size', metavar='NUMBER', type=int, default=5,
                            help="set the pool size to %(metavar)s pilots (default: %(default)s)")
        parser.add_argument('--profile', '-p', default='default', metavar='NAME',
                            help="load the profile %(metavar)s at startup (default: '%(default)s')")
        parser.add_argument('--runner-path', metavar='COMMAND', default=argparse.SUPPRESS,
                            help="specify the location of the runner in the remote nodes", required=True)
        parser.add_argument('--runner-args', metavar='COMMAND', default=argparse.SUPPRESS,
                            help="specify the arguments for the remote runner", required=True)
        parser.add_argument('--version', '-v', action='version', 
                            version='%%(prog)s %s' % release.__version__)
        
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
    
    def _dispatch(self, pool_size, runner_path, runner_args, ce_queue, notify_failed = None, archive_logs = None):
        options = [
            '-u', settings['database_url'],
	]

        options.append(runner_args)

        arguments = ' '.join(options)
        
        job_ids = []
        job_status = collections.defaultdict(int)
        
        # FIXME: job_ids could be lost on KeyboardInterrupt
        try:
            log.info("Launching the initial pilots...")
            for _ in range(pool_size):
                with self._write_jdl(runner_path, arguments) as jdl_file:
                    job_ids.append(glite.ce.job.submit(jdl_file, endpoint=ce_queue))
                    log.debug("Launched a new job with id %s." % job_ids[-1])
            
            while True:
                for st in glite.ce.job.CEJobStatus.processing:
                    job_status[st] = 0
                
                for jobid in job_ids[:]:
                    status = glite.ce.job.status(endpoint=ce_queue.split('/')[0], jobid=jobid)
                    job_status[status.attrs['Status']] += 1
                    log.debug("Job %s is in status %s." % (jobid, status.attrs['Status']))
                    
                    if status.attrs['Status'] in glite.ce.job.CEJobStatus.final:
                        job_ids.remove(jobid)
                        with self._write_jdl(runner_path, arguments) as jdl_file:
                            job_ids.append(glite.ce.job.submit(jdl_file, endpoint=ce_queue))
                            log.debug("Launched a new job with id %s." % job_ids[-1])
                
                msg = ["Pilot summary:"]
                for st in glite.ce.job.CEJobStatus.all:
                    msg.append("%s(%d)" % (st, job_status[st]))
                log.info(" ".join(msg))
                
                time.sleep(10)
        
        finally:
            log.warning("Shutting down pool. DO NOT INTERRUPT.")
            while job_ids:
                for jobid in job_ids:
                    log.debug("Cancelling job %s" % jobid)
                    try:
                        glite.ce.job.cancel(jobid)
                    except Exception:
                        pass
                
                for st in glite.ce.job.CEJobStatus.processing:
                    job_status[st] = 0
                
                for jobid in job_ids[:]:
                    status = glite.ce.job.status(endpoint=ce_queue.split('/')[0], jobid=jobid)
                    job_status[status.attrs['Status']] += 1
                    log.debug("Job %s is in status %s." % (jobid, status.attrs['Status']))
                    
                    if status.attrs['Status'] in glite.ce.job.CEJobStatus.final:
                        job_ids.remove(jobid)
                
                msg = ["Pilot summary:"]
                for st in glite.ce.job.CEJobStatus.all:
                    msg.append("%s(%d)" % (st, job_status[st]))
                log.info(" ".join(msg))
                
                if not job_ids:
                    break
                
                time.sleep(5)
    
    def main(self, args = []):
        if not args:
            args = sys.argv[1:]
        
        parser = self._get_arg_parser()
        options = vars(parser.parse_args(args))
        
        ce_queue    = options.pop('ce_queue', None)
        pool_size   = options.pop('pool_size', None)
        runner_path = options.pop('runner_path', None)
        runner_args = options.pop('runner_args', None)
        
        print "brownthrower dispatcher static v{version} is loading...".format(
            version = release.__version__
        )
        
        api.init(options)
        
        try:
            self._dispatch(pool_size, runner_path, runner_args, ce_queue)
        except KeyboardInterrupt:
            pass
        
def main(args = []):
    dispatcher = StaticDispatcher()
    dispatcher.main(args)

if __name__ == '__main__':
    main()
