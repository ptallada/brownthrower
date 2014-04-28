#!/usr/bin/env python
# -*- coding: utf-8 -*-

from sqlalchemy.orm.exc import NoResultFound

from .model import transactional_session, retry_on_serializable_error
from .job import Job, TaskNotAvailableException, InvalidStatusException

def run(job_id):
    session_maker = None
    
    @retry_on_serializable_error
    def _process_job(job_id):
        with transactional_session(session_maker) as session:
            job = session.query(Job).filter_by(id = job_id).one()
            job.process()
    
    def _run_job(job_id):
        with transactional_session(session_maker) as session:
            # Retrieve job
            job = session.query(Job).filter_by(id = job_id).one()
            if not job.subjobs:
                subjobs = job.prolog()
                if subjobs:
                    print "warning: deprecated"
                    #TODO: create subjobs using compat code
                    return
                
                else:
                    job.run()
            else:
                children = job.epilog()
                if children:
                    print "warning: deprecated"
                    #TODO: create children using compat code
    
    @retry_on_serializable_error
    def _finish_job(job_id, exc):
        with transactional_session(session_maker) as session:
            job = session.query(Job).filter_by(id = job_id).one()
            job.finish(exc)
    
    try:
        # TODO: submit before processing
        _process_job(job_id)
        try:
            _run_job(job_id)
        except BaseException as e:
            _finish_job(job_id, e)
            if isinstance(e, BaseException):
                raise
        else:
            _finish_job(job_id)
    
    except Exception as e:
        try:
            raise
        except InvalidStatusException:
            print e.message
        except TaskNotAvailableException:
            print e.message
        except NoResultFound:
            print "The specified job does not exist."
        finally:
            #log.debug(e)
            print e

