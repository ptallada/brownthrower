#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import brownthrower as bt
import logging
import sys

from sqlalchemy.orm import scoped_session
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.orm.session import sessionmaker

def run(engine, job_id):
    session_maker = scoped_session(sessionmaker(engine))
    
    @bt.retry_on_serializable_error
    def _process_job(job_id):
        with bt.transactional_session(session_maker) as session:
            job = session.query(bt.Job).filter_by(id = job_id).one()
            job.process()
    
    def _run_job(job_id):
        with bt.transactional_session(session_maker) as session:
            # Retrieve job
            job = session.query(bt.Job).filter_by(id = job_id).one()
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
    
    @bt.retry_on_serializable_error
    def _finish_job(job_id, exc):
        with bt.transactional_session(session_maker) as session:
            job = session.query(bt.Job).filter_by(id = job_id).one()
            job.finish(exc)
    
    try:
        # TODO: submit before processing
        exc = None
        _process_job(job_id)
        try:
            _run_job(job_id)
        except Exception as exc:
            pass
        except BaseException as exc:
            raise
        finally:
            _finish_job(job_id, exc)
    
    except Exception as e:
        try:
            raise
        except bt.InvalidStatusException:
            print e.message
        except bt.TaskNotAvailableException:
            print e.message
        except NoResultFound:
            print "The specified job does not exist."
        finally:
            #log.debug(e)
            print e

def _parse_args(args = None):
    parser = argparse.ArgumentParser(prog='runner.serial', add_help=False)
    parser.add_argument('--database-url', '-u', default="sqlite:///", metavar='URL',
                        help="use the settings in %(metavar)s to establish the database connection (default: '%(default)s')")
    #parser.add_argument('--debug', '-d', const='pdb', nargs='?', default=argparse.SUPPRESS,
    #                    help="enable debugging framework (deactivated by default, '%(const)s' if no specific framework is requested)",
    #                    choices=['pydevd', 'ipdb', 'rpdb', 'pdb'])
    #parser.add_argument('--editor', default=argparse.SUPPRESS, metavar='COMMAND',
    #                    help='use %(metavar)s to edit text files')
    parser.add_argument('--help', '-h', action='help',
                        help='show this help message and exit')
    #parser.add_argument('--history-length', type=int, default=argparse.SUPPRESS, metavar='NUMBER',
    #                    help='preserve as many as %(metavar)s lines of history')
    #parser.add_argument('--pager', default=argparse.SUPPRESS, metavar='COMMAND',
    #                    help='use %(metavar)s to display large chunks of text')
    parser.add_argument('--job-id', '-j', type=int, default=argparse.SUPPRESS, metavar='ID',
                        help="run only the job identified by %(metavar)s")
    parser.add_argument('--version', '-v', action='version', 
                        version='%%(prog)s %s' % bt.release.__version__)
    
    options = vars(parser.parse_args(args))
    
    return options

def main(args = None):
    if not args:
        args = sys.argv[1:]
    
    options = _parse_args(args)
    
    # TODO: Add debugging option
    from pysrc import pydevd
    pydevd.settrace()
    
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
    
    print "brownthrower runner serial v{version} is loading...".format(
        version = bt.release.__version__
    )
    
    db_url = options.get('database_url')
    engine = bt.create_engine(db_url)
    job_id = options.get('job_id')
    
    run(engine, job_id)

if __name__ == '__main__':
    main()
