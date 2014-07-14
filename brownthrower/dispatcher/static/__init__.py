#! /usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import collections
import contextlib
import glite.ce.job
import logging
import signal
import string
import sys
import tempfile
import threading
import time

from functools import wraps
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm.session import sessionmaker

import brownthrower as bt

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

def retry(tries):
    def retry_decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            for _ in range(tries - 1):
                try:
                    value = fn(*args, **kwargs)
                    return value
                except Exception as e:
                    log.warning("Exception «%s» caught while calling %s. Retrying..." % (e, fn))
            
            value = fn(*args, **kwargs)
            return value
        
        return wrapper
    return retry_decorator

class StaticDispatcher(object):
    """\
    TODO
    
    TODO
    TODO
    """
    
    __brownthrower_name__ = 'static'
    
    def _parse_args(self, args = None):
        parser = argparse.ArgumentParser(prog='dispatcher.static', add_help=False)
        parser.add_argument('--database-url', '-u', required=True, metavar='URL',
            help="use the settings in %(metavar)s to establish the database connection")
        parser.add_argument('--ce-queue',    metavar='ENDPOINT', default=argparse.SUPPRESS,
            help="select the batch queue to sent the pilots into", required=True)
        parser.add_argument('--help', '-h', action='help',
            help='show this help message and exit')
        parser.add_argument('--pool-size',   metavar='NUMBER',   default=5, type=int,
            help="set the pool size to %(metavar)s pilots (default: %(default)s)")
        parser.add_argument('--runner-path', metavar='COMMAND',  default=argparse.SUPPRESS,
            help="specify the location of the runner in the remote nodes", required=True)
        parser.add_argument('--runner-args', metavar='COMMAND',  default=argparse.SUPPRESS,
            help="specify the arguments for the remote runner", required=True)
        parser.add_argument('--version', '-v', action='version', 
            version='%%(prog)s %s' % bt.release.__version__)
        
        options = vars(parser.parse_args(args))
        
        return options
    
    def _system_exit(self, *args, **kwargs):
        if self._lock.acquire(False):
            log.warning("Caught signal. Terminating...")
            sys.exit(1)
        else:
            log.warning("Caught signal. Terminating already in progress...")
    
    def __init__(self, args):
        options = self._parse_args(args)
        
        self._db_url = options.get('database_url')
        engine = bt.create_engine(self._db_url)
        self._session_maker = scoped_session(sessionmaker(engine))
        
        arguments = [
            '-u', self._db_url,
        ]
        arguments.append(options.pop('runner_args'))
        self._runner_args = ' '.join(arguments)
        
        self._ce_queue      = options.pop('ce_queue')
        self._pool_size     = options.pop('pool_size')
        self._runner_path   = options.pop('runner_path')
        
        self._pilots = {}
        self._last_event = 0
        self._lock = threading.Lock()
        
        signal.signal(signal.SIGINT,  self._system_exit)
        signal.signal(signal.SIGTERM, self._system_exit)
    
    @retry(tries = 3)
    def _init_status(self):
        self._last_event = glite.ce.last_event_id(self._ce_queue.split('/')[0])
    
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
    
    @retry(tries = 3)
    def _launch_pilot(self):
        with self._write_jdl(self._runner_path, self._runner_args) as jdl_file:
            pilot_id = glite.ce.job.submit(jdl_file, endpoint=self._ce_queue)
            self._pilots[pilot_id] = glite.ce.job.CEJobStatus.UNKNOWN
        
        return pilot_id
    
    @retry(tries = 3)
    def _update_status(self):
        finished = 0
        
        for event in glite.ce.event_query(self._ce_queue.split('/')[0], self._last_event):
            if event['jobId'] in self._pilots:
                self._pilots[event['jobId']] = event['status']
                
                if event['status'] in glite.ce.job.CEJobStatus.final:
                    finished += 1
        
        self._last_event = int(event['EventID'])
        
        return finished
    
    def _build_summary(self):
        job_status = collections.defaultdict(int)
        
        job_status.clear()
        for status in self._pilots.itervalues():
            job_status[status] += 1
        
        msg = ["Pilot summary:"]
        for status, count in job_status.iteritems():
            msg.append("%s(%d)" % (status, count))
        
        return " ".join(msg)
    
    def _cancel_pilots(self):
        for pilot_id, status in self._pilots.copy().iteritems():
            if status not in glite.ce.job.CEJobStatus.final:
                try:
                    glite.ce.job.cancel(pilot_id)
                except Exception:
                    pass
    
    def _clear_finished_pilots(self):
        for pilot_id, status in self._pilots.copy().iteritems():
            if status in glite.ce.job.CEJobStatus.final:
                del self._pilots[pilot_id]
    
    def main(self):
        try:
            self._init_status()
            
            log.info("Launching the initial pilots...")
            for _ in range(self._pool_size):
                pilot_id = self._launch_pilot()
                log.debug("Launched a new job with id %s." % pilot_id)
            
            while True:
                finished = self._update_status()
                
                log.info("Launching %s additional pilots..." % finished)
                for _ in range(finished):
                    pilot_id = self._launch_pilot()
                    log.debug("Launched a new job with id %s." % pilot_id)
                
                summary = self._build_summary()
                log.info(summary)
                
                time.sleep(5)
        
        finally:
            log.warning("Shutting down pool. DO NOT INTERRUPT.")
            while self._pilots:
                log.debug("Cancelling active pilots, please wait...")
                self._cancel_pilots()
                self._update_status()
                self._clear_finished_pilots()
                
                if not self._pilots:
                    break
                
                summary = self._build_summary()
                log.info(summary)
                
                time.sleep(10)

def main(args=None):
    if not args:
        args = sys.argv[1:]
    
    # TODO: Add debugging option
    #from pysrc import pydevd
    #pydevd.settrace()
    
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
    
    dispatcher = StaticDispatcher(args)
    try:
        dispatcher.main()
    except KeyboardInterrupt:
        print

if __name__ == '__main__':
    sys.exit(main(sys.argv))
