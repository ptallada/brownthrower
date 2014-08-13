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
import trunk

from sqlalchemy.orm import scoped_session
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.orm.session import sessionmaker
from trunk import Trunk

from . import process
from . import utils

log = logging.getLogger('brownthrower.runner.serial')

class NoRunnableJobFound(Exception):
    pass

class SerialRunner(object):
    
    def _parse_args(self, args = None):
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
        
        parser.add_argument('--submit', '-s', action='store_true',
            help='in conjunction with --job-id, submit the job before executing')
        parser.add_argument('--version', '-v', action='version', 
            version='%%(prog)s %s' % bt.release.__version__)
        
        options = vars(parser.parse_args(args))
        
        return options
    
    def _system_exit(self, *args, **kwargs):
        if self._lock.acquire(False):
            log.warning("Caught signal. Terminating...")
            sys.exit(0)
        else:
            log.warning("Caught signal. Terminating already in progress...")
    
    def __init__(self, args):
        options = self._parse_args(args)
        db_url = options.get('database_url')
        
        self._session_maker = bt.session_maker(db_url)
        self._job_id        = options.pop('job_id', None)
        self._loop          = options.pop('loop', 0)
        self._notify_failed = options.pop('notify_failed', None)
        self._post_mortem   = options.pop('post_mortem', None)
        self._submit        = options.pop('submit', False)
        
        self._lock = threading.Lock()
        
        signal.signal(signal.SIGINT,  self._system_exit)
        signal.signal(signal.SIGTERM, self._system_exit)
    
    def _get_runnable_job_ids(self):
        with bt.transactional_session(self._session_maker) as session:
            return session.query(bt.Job.id).filter(
                bt.Job.status == bt.Job.Status.QUEUED,
                bt.Job.task.in_(bt.tasks.keys()), # @UndefinedVariable
                ~ bt.Job.parents.any(bt.Job.status != bt.Job.Status.DONE) # @UndefinedVariable
            ).all()
    
    def _run_job(self, job_id, q_finish, q_cancel, submit=False):
        proc = process.Monitor(
            db_url   = self._session_maker.bind.url,
            job_id   = job_id,
            submit   = submit,
            q_finish = q_finish,
        )
        proc.start()
        
        try:
            while True:
                try:
                    r, _, _ = select.select([q_cancel, q_finish], [], [], None)
                    
                    if q_cancel in r:
                        if q_cancel.get() == self._job_id:
                            proc.terminate()
                    if not q_finish.empty():
                        q_finish.get()
                        break
                
                except select.error as e:
                    if e.args[0] != errno.EINTR:
                        raise
        
        except BaseException:
            proc.terminate()
            raise
        finally:
            proc.join()
    
    def _run_one(self, q_finish, q_cancel):
        for job_id in self._get_runnable_job_ids():
            try:
                self._run_job(job_id, q_finish, q_cancel)
                return
            except bt.InvalidStatusException:
                pass
        
        raise NoRunnableJobFound()
    
    def _run_all(self, q_finish, q_cancel):
        while True:
            try:
                self._run_one(q_finish, q_cancel)
            except NoRunnableJobFound:
                break
    
    def main(self):
        q_finish = utils.SelectableQueue()
        if self._session_maker.bind.url.drivername == 'postgresql':
            q_cancel = utils.CancelQueue(self._session_maker.bind.url)
        else:
            # Fallback dummy implementation
            q_cancel = utils.SelectableQueue()
        
        try:
            if self._job_id:
                self._run_job(self._job_id, q_finish, q_cancel, self._submit)
            else:
                while True:
                    self._run_all(q_finish, q_cancel)
                    
                    if not self._loop:
                        return
                    
                    log.info("No runnable jobs found. Sleeping %d seconds until next iteration." % self._loop)
                    time.sleep(self._loop)
        
        except BaseException as e:
            try:
                raise
#             except NoResultFound:
#                 pass
#             except bt.InvalidStatusException:
#                 pass
            finally:
                pass
#                 #FIXME: que cal fer aqui? com gestionar els errors. per exemple, l'id no existeix. o el job indicat no te l'estat que toca.
#                 #log.debug(e, exc_info=True)

def main(args=None):
    if not args:
        args = sys.argv[1:]
    
    # TODO: Add debugging option
    #from pysrc import pydevd
    #pydevd.settrace(port=5678)
    
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
    
    runner = SerialRunner(args)
    try:
        runner.main()
    except SystemExit:
        print

if __name__ == '__main__':
    sys.exit(main(sys.argv))
