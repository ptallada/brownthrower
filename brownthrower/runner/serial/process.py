#!/usr/bin/env python
# -*- coding: utf-8 -*-

import brownthrower as bt
import errno
import logging
import multiprocessing
import os
import signal
import sys
import threading

from sqlalchemy.orm import undefer_group
from sqlalchemy.orm.exc import NoResultFound

log = logging.getLogger('brownthrower.runner.serial')

# Number of seconds to wait between SIGTERM and SIGKILL when terminating a job
KILL_TIMEOUT=2

class Job(multiprocessing.Process):
    def __init__(self, db_url, job_id, token):
        super(Job, self).__init__(name='bt_job_%d' % job_id)
        self._job_id = job_id
        self._db_url = db_url
        self._token  = token
        self._lock   = threading.Lock()
    
    def _system_exit(self, *args, **kwargs):
        if self._lock.acquire(False):
            log.warning("Caught signal in job. Terminating...")
            sys.exit(0)
        else:
            log.warning("Caught signal in job. Terminating already in progress...")
    
    def run(self):
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, self._system_exit)
        
        try:
            session_maker = bt.session_maker(self._db_url)
            with bt.transactional_session(session_maker) as session:
                job = session.query(bt.Job).filter_by(
                    id = self._job_id
                ).options(undefer_group('yaml')).one()
                
                job.run(self._token)
        
        except:
            log.debug("An error was found running job %d" % self._job_id, exc_info=True)
    
    def cancel(self):
        if self.is_alive():
            self.terminate()
            self.join(timeout=KILL_TIMEOUT)
            # send SIGKILL if still alive
            if self.exitcode == None:
                try:
                    os.kill(self.pid, signal.SIGKILL)
                except OSError as e:
                    if e.errno != errno.ESRCH:
                        raise
            self.join()
    
    @bt.retry_on_serializable_error
    def _cleanup_job(self, tb=None):
        session_maker = bt.session_maker(self._db_url)
        with bt.transactional_session(session_maker) as session:
            job = session.query(bt.Job).filter_by(id = self._job_id).one()
            job.cleanup(self._token, tb)
    
    def cleanup(self):
        try:
            self._cleanup_job("Job aborted with exit code %d" % self.exitcode)
        except (bt.InvalidStatusException, bt.TokenMismatchException, NoResultFound):
            pass

class Monitor(multiprocessing.Process):
    
    def __init__(self, db_url, job_id, q_finish, token, submit=False):
        super(Monitor, self).__init__(name='bt_monitor_%d' % job_id)
        self._job_id = job_id
        self._db_url = db_url
        self._q_finish = q_finish
        self._token  = token
        self._submit = submit
        self._lock   = threading.Lock()
    
    def _system_exit(self, *args, **kwargs):
        if self._lock.acquire(False):
            log.warning("Caught signal in monitor. Terminating...")
            sys.exit(0)
        else:
            log.warning("Caught signal in monitor. Terminating already in progress...")
    
    @bt.retry_on_serializable_error
    def _reserve_job(self):
        session_maker = bt.session_maker(self._db_url)
        with bt.transactional_session(session_maker) as session:
            job = session.query(bt.Job).filter_by(
                id = self._job_id
            ).one()
            
            if self._submit:
                job.submit()
            
            job.assert_is_available()
            job.reserve(self._token)
    
    def run(self):
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, self._system_exit)
        
        job_process = Job(
            db_url = self._db_url,
            job_id = self._job_id,
            token  = self._token,
        )
        
        try:
            job_process.start()
            job_process.join()
        except SystemExit:
            job_process.cancel()
        finally:
            try:
                job_process.cleanup()
            finally:
                self._q_finish.put(self._job_id)
    
    def start(self):
        self._reserve_job()
        super(Monitor, self).start()
