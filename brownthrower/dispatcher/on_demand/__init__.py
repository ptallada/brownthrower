#! /usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import logging
import signal
import sys
import threading

import brownthrower as bt
import brownthrower.utils as utils

from collections import defaultdict

from . import threads
from . import ui

log = logging.getLogger('brownthrower.dispatcher.on_demand')

class LockedContainer(object):
    def __init__(self, container):
        self._rlock = threading.RLock()
        self._container = container
    
    def __enter__(self):
        self._rlock.acquire()
        return self._container
    
    def __exit__(self, exc_type, exc_value, traceback):
        self._rlock.release()

class OnDemandDispatcher(object):
    
    def _parse_args(self, args = None):
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
            help="only those tasks will be eligible for running", required=True)
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
        
        self._bt_ids = LockedContainer(defaultdict(dict))
        self._bt_status = LockedContainer(defaultdict(int))
        self._glite_ids = LockedContainer(defaultdict(dict))
        self._glite_status = LockedContainer(defaultdict(int))
        
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
        
        self._ui = ui.MainScreen()
        self._ui.set_callback(self._refresh, self._update_ui)
    
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

def main(args=None):
    if not args:
        args = sys.argv[1:]
    
    # TODO: Add debugging option
    #from pysrc import pydevd
    #pydevd.settrace()
    
    #logging.basicConfig(level=logging.DEBUG)
    #logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
    
    dispatcher = OnDemandDispatcher(args)
    try:
        dispatcher.run()
    except KeyboardInterrupt:
        print

if __name__ == '__main__':
    sys.exit(main())
