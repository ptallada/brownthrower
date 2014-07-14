#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import brownthrower as bt
import logging
import signal
import sys
import threading
import time
import traceback

from sqlalchemy.orm import scoped_session
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.orm.session import sessionmaker

log = logging.getLogger('brownthrower.runner.serial')

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
            sys.exit(1)
        else:
            log.warning("Caught signal. Terminating already in progress...")
    
    def __init__(self, args):
        options = self._parse_args(args)
        db_url = options.get('database_url')
        engine = bt.create_engine(db_url)
        
        self._session_maker = scoped_session(sessionmaker(engine))
        self._job_id        = options.pop('job_id', None)
        self._loop          = options.pop('loop', 0)
        self._notify_failed = options.pop('notify_failed', None)
        self._post_mortem   = options.pop('post_mortem', None)
        self._submit        = options.pop('submit', False)
        
        self._lock = threading.Lock()
        
        signal.signal(signal.SIGINT,  self._system_exit)
        signal.signal(signal.SIGTERM, self._system_exit)
    
    def _run_job(self, job_id, submit=False):
        @bt.retry_on_serializable_error
        def _process(job_id, submit=False):
            with bt.transactional_session(self._session_maker) as session:
                job = session.query(bt.Job).filter_by(id = job_id).one()
                if submit:
                    job.submit()
                job.process()
        
        def _run(job_id):
            with bt.transactional_session(self._session_maker) as session:
                # Retrieve job
                job = session.query(bt.Job).filter_by(id = job_id).one()
                if not job.subjobs:
                    job.prolog()
                    if not job.subjobs:
                        job.run()
                
                else:
                    job.epilog()
        
        @bt.retry_on_serializable_error
        def _finish(job_id, exc):
            with bt.transactional_session(self._session_maker) as session:
                job = session.query(bt.Job).filter_by(id = job_id).one()
                job.finish(exc)
        
        exc = None
        _process(job_id, submit)
        try:
            _run(job_id)
        except BaseException as exc:
            tb = traceback.format_exc()
            log.debug(tb)
            try:
                raise
            except Exception:
                pass
        finally:
            _finish(job_id, exc)
    
    def _get_runnable_job_ids(self):
        with bt.transactional_session(self._session_maker) as session:
            return session.query(bt.Job.id).filter(
                bt.Job.status == bt.Job.Status.QUEUED,
                bt.Job.task.in_(bt.tasks.keys()),
                ~ bt.Job.parents.any(bt.Job.status != bt.Job.Status.DONE) # @UndefinedVariable
            ).all()
    
    def main(self):
        if not self._job_id:
            while True:
                while True:
                    for job_id in self._get_runnable_job_ids():
                        try:
                            self._run_job(job_id)
                        except Exception:
                            continue
                        else:
                            break
                
                if not self._loop:
                    break
                
                time.sleep(self._loop)
        else:
            try:
                self._run_job(self._job_id, self._submit)
            except BaseException as e:
                try:
                    raise
                except bt.InvalidStatusException:
                    print e.message
                except bt.TaskNotAvailableException:
                    print e.message
                except NoResultFound:
                    print "The specified job does not exist."
                finally:
                    log.debug(e)

def main(args=None):
    if not args:
        args = sys.argv[1:]
    
    # TODO: Add debugging option
    from pysrc import pydevd
    pydevd.settrace()
    
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
    
    runner = SerialRunner(args)
    try:
        runner.main()
    except KeyboardInterrupt:
        print

if __name__ == '__main__':
    sys.exit(main(sys.argv))
