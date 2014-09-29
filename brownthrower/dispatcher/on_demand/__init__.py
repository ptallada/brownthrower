#! /usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import logging
import signal
import sys
import threading

from collections import defaultdict

import brownthrower as bt
import brownthrower.utils as utils

from . import threads
from . import ui

log = logging.getLogger('brownthrower.dispatcher.on_demand')

class OnDemandDispatcher(object):
    
    def __init__(self, args):
        options = self._parse_args(args)
        
        self._db_url = options.get('database_url')
        self._session_maker = bt.session_maker(self._db_url)
        
        arguments = [
            '-u', self._db_url,
        ]
        arguments.append(options.pop('runner_args'))
        self._runner_args = ' '.join(arguments)
        
        self._ce_queue      = options.pop('ce_queue')
        self._runner_path   = options.pop('runner_path')
        self._allowed_tasks = set(map(str.strip, options.pop('allowed_tasks').split(',')))
        
        self._lock = threading.Lock()
        
        signal.signal(signal.SIGINT,  self._system_exit)
        signal.signal(signal.SIGTERM, self._system_exit)
        
        self._bt_ids = utils.LockedContainer(defaultdict(dict))
        self._bt_status = utils.LockedContainer(defaultdict(int))
        self._glite_ids = utils.LockedContainer(defaultdict(dict))
        self._glite_status = utils.LockedContainer(defaultdict(int))
        
        self._refresh = utils.SelectableQueue()
        
        self._launcher = threads.LauncherThread(
            self._session_maker,
            self._allowed_tasks,
            self._runner_path,
            self._runner_args,
            self._ce_queue,
            self._bt_status,
            self._bt_ids,
            self._glite_status,
            self._glite_ids,
            self._refresh
        )
        self._bt_monitor = threads.BtMonitorThread(
            self._session_maker, 
            self._bt_ids,
            self._bt_status,
            self._refresh
        )
        self._glite_monitor = threads.GliteMonitorThread(
            self._ce_queue,
            self._glite_ids,
            self._glite_status,
            self._refresh
        )
        
        self._ui = ui.MainScreen(
            self._runner_path,
            self._runner_args,
            self._ce_queue,
            self._allowed_tasks,
        )
        self._ui.set_callback(self._refresh, self._update_ui)
    
    def _system_exit(self, *args, **kwargs):
        if self._lock.acquire(False):
            log.warning("Caught signal. Terminating...")
            sys.exit(0)
        else:
            log.warning("Caught signal. Terminating already in progress...")
    
    def _update_ui(self):
        self._refresh.get()
        with self._bt_status as bt_status:
            self._ui.update_bt(bt_status)
        with self._glite_status as glite_status:
            self._ui.update_glite(glite_status)
    
    def run(self, *args, **kwargs):
        try:
            self._launcher.start()
            self._bt_monitor.start()
            self._glite_monitor.start()
            
            self._ui.run()
        
        finally:
            if self._launcher.is_alive():
                self._launcher.stop()
            if self._bt_monitor.is_alive():
                self._bt_monitor.stop()
            if self._glite_monitor.is_alive():
                self._glite_monitor.stop()
            if self._launcher.is_alive():
                self._launcher.join()
            if self._bt_monitor.is_alive():
                self._bt_monitor.join()
            if self._glite_monitor.is_alive():
                self._glite_monitor.join()

def _parse_args(args = None):
    parser = argparse.ArgumentParser(prog='dispatcher.on_demand', add_help=False)
    parser.add_argument('--database-url', '-u', required=True, metavar='URL',
        help="database connection settings")
    parser.add_argument('--ce-queue',    metavar='ENDPOINT', default=argparse.SUPPRESS,
        help="submit the pilot jobs to %(metavar)s", required=True)
    parser.add_argument('--help', '-h', action='help',
        help='show this help message and exit')
    parser.add_argument('--runner-path', metavar='COMMAND',  default=argparse.SUPPRESS,
        help="full path of the runner in the remote nodes", required=True)
    parser.add_argument('--runner-args', metavar='ARG_LIST',  default=argparse.SUPPRESS,
        help="extra arguments to provide to the remote runner", required=True)
    parser.add_argument('--allowed-tasks', metavar='NAME_LIST',  default=argparse.SUPPRESS,
        help="comma-separated list of tasks eligible for running", required=True)
    parser.add_argument('--verbose', '-v', action='count', default=0,
        help='increment verbosity level (can be specified twice)')
    parser.add_argument('--version', action='version', 
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
    #pydevd.settrace()
    
    dispatcher = OnDemandDispatcher(options)
    try:
        dispatcher.run()
    except KeyboardInterrupt:
        print

if __name__ == '__main__':
    sys.exit(main())
