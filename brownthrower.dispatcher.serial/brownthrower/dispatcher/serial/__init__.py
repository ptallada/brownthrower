#! /usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import logging
import sys
import time
import traceback
import transaction

from . import release
from brownthrower import api, interface
from brownthrower.api.profile import settings

try:
    from logging import NullHandler
except ImportError:
    from logutils import NullHandler # @UnusedImport

log = logging.getLogger('brownthrower.dispatcher.serial')
log.addHandler(NullHandler())

class SerialDispatcher(interface.dispatcher.Dispatcher):
    """\
    Basic serial dispatcher for testing and development.
    
    This dispatcher executes the jobs one by one in succession.
    It supports both SQLite and PostgreSQL.
    """
    
    __brownthrower_name__ = 'serial'
    
    def _parse_args(self, args):
        parser = argparse.ArgumentParser(prog='dispatcher.serial', add_help=False)
        parser.add_argument('--database-url', '-u', default=argparse.SUPPRESS, metavar='URL',
                            help='use the settings in %(metavar)s to establish the database connection')
        parser.add_argument('--debug', '-d', const='pdb', nargs='?', default=argparse.SUPPRESS,
                            help="enable debugging framework (deactivated by default, '%(const)s' if no specific framework is requested)",
                            choices=['pydevd', 'ipdb', 'rpdb', 'pdb'])
        parser.add_argument('--help', '-h', action='help',
                            help='show this help message and exit')
        group = parser.add_mutually_exclusive_group()
        group.add_argument('--job-id', '-j', type=int, default=argparse.SUPPRESS, metavar='ID',
                            help="run only the job identified by %(metavar)s")
        group.add_argument('--loop', metavar='NUMBER', nargs='?', type=int, const=60, default=argparse.SUPPRESS,
                            help="enable infinite looping, waiting %(metavar)s seconds between iterations (default: %(const)s)")
        parser.add_argument('--post-mortem', const='pdb', nargs='?', default=argparse.SUPPRESS,
                            help="enable post-mortem debugging (deactivated by default, '%(const)s' if no specific framework is requested)",
                            choices=['ipdb', 'pdb'])
        parser.add_argument('--profile', '-p', default='default', metavar='NAME',
                            help="load the profile %(metavar)s at startup (default: '%(default)s')")
        parser.add_argument('--version', '-v', action='version', 
                            version='%%(prog)s %s' % release.__version__)
        
        options = vars(parser.parse_args(args))
        
        return options
    
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
    
    def _run_job(self, post_mortem = None, job_id = None):
        try:
            (job, ancestors) = api.dispatcher.get_runnable_job(job_id)
            log.info("Job %d has been locked and it is being processed." % job.id)
        except BaseException:
            transaction.abort()
            raise
        
        try:
            try:
                task = api.dispatcher.process_job(job, ancestors)
                preloaded_job = api.dispatcher.preload_job(job)
                transaction.commit()
            except BaseException:
                transaction.abort()
                raise
            
            log.info("Job %d is now in PROCESSING state and it is being run." % preloaded_job.id)
            
            with transaction.manager:
                api.dispatcher.run_job(preloaded_job, task)
            
            log.info("Job %d has finished successfully and it is now in DONE state." % preloaded_job.id)
        
        except BaseException as e:
            ex = traceback.format_exc()
            log.debug(ex)
            
            try:
                api.dispatcher.handle_job_exception(preloaded_job, e)
            finally:
                transaction.commit()
                log.warning("Job %d was aborted with status '%s'." % (preloaded_job.id, preloaded_job.status))
                
            # Enter post-mortem
            if post_mortem and preloaded_job.status == interface.constants.JobStatus.FAILED:
                self._enter_postmortem(post_mortem)
    
    def run(self, args = []):
        options = self._parse_args(args)
        
        job_id = options.pop('job_id', None)
        loop = options.pop('loop', 0)
        post_mortem = options.pop('post_mortem', None)
        
        api.init(options)
        
        try:
            if job_id:
                self._run_job(post_mortem, job_id)
                return
            
            while True:
                try:
                    while True:
                        self._run_job(post_mortem)
                except api.dispatcher.NoRunnableJobFound:
                    pass
                
                if not loop:
                    return
                
                log.info("No runnable jobs found. Sleeping %d seconds until next iteration." % loop)
                time.sleep(loop)
        
        except KeyboardInterrupt:
            pass

def main(args = None):
    if not args:
        args = sys.argv[1:]
    
    print "brownthrower dispatcher serial v{version} is loading...".format(
        version = release.__version__
    )
    dispatcher = SerialDispatcher()
    dispatcher.run(args)

if __name__ == '__main__':
    main()
