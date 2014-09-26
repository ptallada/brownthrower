#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import brownthrower as bt
import contextlib
import errno
import logging
import multiprocessing
import multiprocessing.queues
import os
import select
import signal
import sys
import threading
import time
import uuid

from brownthrower.utils import SelectableQueue
from sqlalchemy.orm.exc import NoResultFound

from . import process

log = logging.getLogger('brownthrower.runner.serial')

class NoRunnableJobFound(Exception):
    pass

class SerialRunner(object):
    
    def __init__(self, options):
        db_url = options.get('database_url')
        
        self._session_maker = bt.session_maker(db_url)
        self._job_id        = options.pop('job_id', None)
        self._loop          = options.pop('loop', 0)
        self._notify_failed = options.pop('notify_failed', None)
        self._post_mortem   = options.pop('post_mortem', None)
        self._submit        = options.pop('submit', False)
        self._token         = options.pop('reserved', uuid.uuid1().hex)
        
        self._lock = threading.Lock()
        
        signal.signal(signal.SIGINT,  self._system_exit)
        signal.signal(signal.SIGTERM, self._system_exit)
    
    def _system_exit(self, *args, **kwargs):
        if self._lock.acquire(False):
            log.warning("Caught signal. Terminating...")
            sys.exit(0)
        else:
            log.warning("Caught signal. Terminating already in progress...")
    
    def _must_terminate(self, job_id):
        with bt.transactional_session(self._session_maker) as session:
            job = session.query(bt.Job).filter_by(
                id = job_id,
                token = self._token,
                status = bt.Job.Status.RUNNING,
            ).first()
            
            return not bool(job)
    
    def _run_job(self, job_id, q_finish, q_abort, token, submit=False):
        proc = process.Monitor(
            db_url   = self._session_maker.bind.url,
            job_id   = job_id,
            token    = token,
            submit   = submit,
            q_finish = q_finish,
        )
        
        try:
            proc.start()
            
            while True:
                try:
                    r, _, _ = select.select([q_abort, q_finish], [], [], None)
                    if q_abort in r:
                        for _, payload in q_abort:
                            if int(payload) == job_id:
                                if self._must_terminate(job_id):
                                    proc.terminate()
                    if not q_finish.empty():
                        q_finish.get()
                        break
                
                except select.error as e:
                    if e.args[0] != errno.EINTR:
                        raise
            
        except SystemExit:
            if proc.is_alive():
                proc.terminate()
                raise
        finally:
            if proc.is_alive():
                proc.join()
    
    def _run_one(self, q_finish, q_abort):
        with bt.transactional_session(self._session_maker) as session:
            jobs = session.query(bt.Job).filter(
                bt.Job.status == bt.Job.Status.QUEUED,
                bt.Job.name.in_(bt.tasks.keys()), # @UndefinedVariable
                ~ bt.Job.parents.any(bt.Job.status != bt.Job.Status.DONE) # @UndefinedVariable
            )
            job_ids = [job.id for job in jobs]
        
        for job_id in job_ids:
            try:
                self._run_job(job_id, q_finish, q_abort, self._token)
                return
            except bt.InvalidStatusException, NoResultFound:
                pass
        
        raise NoRunnableJobFound()
    
    def _run_all(self, q_finish, q_abort):
        while True:
            try:
                self._run_one(q_finish, q_abort)
            except NoRunnableJobFound:
                break
    
    def main(self):
        q_finish = SelectableQueue()
        if self._session_maker.bind.url.drivername == 'postgresql':
            q_abort = bt.Notifications(self._session_maker)
            q_abort.listen(q_abort.channel.job_delete)
            q_abort.listen(q_abort.channel.job_update)
        else:
            # Fallback dummy implementation
            q_abort = SelectableQueue()
        
        if self._job_id:
            self._run_job(self._job_id, q_finish, q_abort, self._token, self._submit)
        else:
            while True:
                self._run_all(q_finish, q_abort)
                
                if not self._loop:
                    return
                
                log.info("No runnable jobs found. Sleeping %d seconds until next iteration." % self._loop)
                time.sleep(self._loop)

def _parse_args(args = None):
    parser = argparse.ArgumentParser(prog='runner.serial', add_help=False)
    parser.add_argument('--database-url', '-u', required=True, metavar='URL',
        help="use the settings in %(metavar)s to establish the database connection")
    parser.add_argument('--help', '-h', action='help',
        help='show this help message and exit')
    
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--job-id', '-j', type=int, default=argparse.SUPPRESS, metavar='ID',
        help="run only the job identified by %(metavar)s")
    group.add_argument('--loop', metavar='NUMBER', nargs='?', type=int, const=60, default=argparse.SUPPRESS,
        help="enable infinite looping, waiting %(metavar)s seconds between iterations (default: %(const)s)")
    
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--reserved', '-r', default=argparse.SUPPRESS, metavar='TOKEN',
        help='in conjunction with --job-id, run a previously reserved job')
    group.add_argument('--submit', '-s', action='store_true',
        help='in conjunction with --job-id, submit the job before executing')
    
    parser.add_argument('--verbose', '-v', action='count', default=0,
        help='increment verbosity level (can be specified twice)')
    parser.add_argument('--version', '-v', action='version', 
        version='%%(prog)s %s' % bt.release.__version__)
    
    options = vars(parser.parse_args(args))
    
    return options

def main(args=None):
    if not args:
        args = sys.argv[1:]
    
    options = _parse_args(args)
    
    # Configure logging verbosity
    verbosity = options.pop('verbose')
    bt._setup_logging(verbosity)
    
    # TODO: Add debugging option
    #from pysrc import pydevd
    #pydevd.settrace(port=5678)
    
    runner = SerialRunner(options)
    try:
        runner.main()
    except SystemExit:
        print

if __name__ == '__main__':
    sys.exit(main())
