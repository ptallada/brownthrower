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
import traceback

from sqlalchemy.orm import scoped_session
from sqlalchemy.orm.session import sessionmaker

log = logging.getLogger('brownthrower.runner.serial')

# Number of seconds to wait between SIGTERM and SIGKILL when terminating a job
KILL_TIMEOUT=2

class Job(multiprocessing.Process):
    def __init__(self, db_url, job_id):
        super(Job, self).__init__(name='bt_job_%d' % job_id)
        self._job_id = job_id
        self._db_url = db_url
        self._lock   = threading.Lock()
    
    def _system_exit(self, *args, **kwargs):
        if self._lock.acquire(False):
            log.warning("Caught signal in job. Terminating...")
            sys.exit(0)
        else:
            log.warning("Caught signal in job. Terminating already in progress...")
    
    def _run_job(self):
        session_maker = bt.session_maker(self._db_url)
        with bt.transactional_session(session_maker) as session:
            job = session.query(bt.Job).filter_by(id = self._job_id).one()
            if not job.subjobs:
                job.prolog()
                if not job.subjobs:
                    job.run()
            else:
                job.epilog()
    
    @bt.retry_on_serializable_error
    def _finish_job(self, tb):
        session_maker = bt.session_maker(self._db_url)
        with bt.transactional_session(session_maker) as session:
            job = session.query(bt.Job).filter_by(id = self._job_id).one()
            job.finish(tb)
    
    def run(self):
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, self._system_exit)
        
        tb = None
        try:
            self._run_job()
        except BaseException as e:
            log.debug(e, exc_info=True)
            tb = ''.join(traceback.format_exception(*sys.exc_info()))
        finally:
            self._finish_job(tb)
    
    def cancel(self):
        self.terminate()
        self.join(timeout=KILL_TIMEOUT)
        # send SIGKILL if still alive
        if self.exitcode == None:
            try:
                os.kill(self.pid, signal.SIGKILL)
            except OSError as e:
                if e.errno != errno.ESRCH:
                    raise
    
    def finish(self):
        try:
            self._finish_job("Job aborted with exit code %d" % self.exitcode)
        except bt.InvalidStatusException:
            pass

class Monitor(multiprocessing.Process):
    
    def __init__(self, db_url, job_id, q_finish, submit=False):
        super(Monitor, self).__init__(name='bt_monitor_%d' % job_id)
        self._job_id = job_id
        self._db_url = db_url
        self._q_finish = q_finish
        self._submit = submit
        self._lock   = threading.Lock()
    
    def _system_exit(self, *args, **kwargs):
        if self._lock.acquire(False):
            log.warning("Caught signal in monitor. Terminating...")
            sys.exit(0)
        else:
            log.warning("Caught signal in monitor. Terminating already in progress...")
    
    @bt.retry_on_serializable_error
    def _process_job(self):
        session_maker = bt.session_maker(self._db_url)
        with bt.transactional_session(session_maker) as session:
            job = session.query(bt.Job).filter_by(id = self._job_id).one()
            if self._submit:
                job.submit()
            job.process()
    
    def run(self):
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, self._system_exit)
        
        job_process = Job(
            db_url = self._db_url,
            job_id = self._job_id,
        )
        job_process.start()
        
        try:
            job_process.join()
        except BaseException:
            job_process.cancel()
        finally:
            job_process.join()
            job_process.finish()
            self._q_finish.put(self._job_id)
    
    def start(self):
        self._process_job()
        super(Monitor, self).start()
