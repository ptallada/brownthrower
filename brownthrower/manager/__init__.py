#!/usr/bin/env python
# -*- coding: utf-8 -*-

import cmd
import logging
import textwrap
import signal
import sys

from brownthrower import common
from brownthrower import model

# TODO: read and create a global or local configuration file
_CONFIG = {
    'entry_points.dispatcher' : 'brownthrower.dispatcher',
    'entry_points.chain'      : 'brownthrower.chain',
    'entry_points.task'       : 'brownthrower.task',
    'manager.editor'          : 'nano',
    'manager.viewer'          : 'less',
    'database.url'            : 'postgresql://tallada:secret,@db01.pau.pic.es/test_tallada',
    'listing.limit'           : 50,
}

log = logging.getLogger('brownthrower.manager')

class Manager(cmd.Cmd):
    
    def __init__(self, *args, **kwargs):
        cmd.Cmd.__init__(self, *args, **kwargs)
        
        self._dispatchers = {}
        self._chains      = {}
        self._subcmds     = {}
        self._tasks       = {}
        
        self.intro = "\nPAU DM Manager v0.1 is ready"
    
    def preloop(self):
        self._chains      = common.load_chains(     _CONFIG['entry_points.chain'])
        self._dispatchers = common.load_dispatchers(_CONFIG['entry_points.dispatcher'])
        self._tasks       = common.load_tasks(      _CONFIG['entry_points.task'])
        
        from commands import Chain #@UnresolvedImport
        from commands import Cluster #@UnresolvedImport
        from commands import Dispatcher #@UnresolvedImport
        from commands import Job  #@UnresolvedImport
        from commands import Task #@UnresolvedImport
        
        self._subcmds['chain']      = Chain(          chains = self._chains)
        self._subcmds['cluster']    = Cluster(        chains = self._chains,
                                                      editor = _CONFIG['manager.editor'],
                                                      viewer = _CONFIG['manager.viewer'],
                                                       limit = _CONFIG['listing.limit'])
        self._subcmds['dispatcher'] = Dispatcher(dispatchers = self._dispatchers)
        self._subcmds['job']        = Job(             tasks = self._tasks,
                                                      editor = _CONFIG['manager.editor'],
                                                      viewer = _CONFIG['manager.viewer'],
                                                       limit = _CONFIG['listing.limit'])
        self._subcmds['task']       = Task(            tasks = self._tasks)
        
    
    def do_help(self, line):
        items = line.strip().split()
        if items:
            subcmd = self._subcmds.get(items[0])
            if subcmd:
                return subcmd._help(items[1:])
        
        print textwrap.dedent("""\
        usage: <command> [options]
        
        Available commands:
            chain         show information about the available chains
            cluster       create, configure, submit and remove clusters
            dispatcher    show information about the available dispatchers
            job           create, configure, submit and remove jobs
            quit          exit this program
            task          show information about the available tasks
        """)
    
    def completedefault(self, text, line, begidx, endidx):
        items = line[:begidx].strip().split()
        if len(items) > 0:
            subcmd = self._subcmds.get(items[0])
            if subcmd:
                return subcmd._complete(text, items[1:])
    
    def do_chain(self, line):
        items = line.strip().split()
        self._subcmds['chain']._do(items)
    
    def do_cluster(self, line):
        items = line.strip().split()
        self._subcmds['cluster']._do(items)
    
    def do_dispatcher(self, line):
        items = line.strip().split()
        self._subcmds['dispatcher']._do(items)
    
    def do_job(self, line):
        items = line.strip().split()
        self._subcmds['job']._do(items)
    
    def do_task(self, line):
        items = line.strip().split()
        self._subcmds['task']._do(items)
    
    def do_quit(self, line):
        return self.do_EOF(line)
    
    def do_EOF(self, line):
        return True
    
    def postcmd(self, stop, line):
        model.session.rollback()
        return cmd.Cmd.postcmd(self, stop, line)
    
    def postloop(self):
        print

def system_exit(*args, **kwargs):
    sys.exit(1)

def main():
    signal.signal(signal.SIGTERM, system_exit)
    
    # TODO: Remove
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger('sqlalchemy.engine').setLevel(logging.DEBUG)
    
    from pysrc import pydevd
    pydevd.settrace(suspend=False)
    
    #import rpdb
    #rpdb.Rpdb().set_trace()
    
    #model.init(_CONFIG['database.url'])
    model.init('sqlite:////tmp/manager.db')
    #model.Base.metadata.create_all()
    
    manager = Manager()
    manager.cmdloop()

if __name__ == '__main__':
    main()
