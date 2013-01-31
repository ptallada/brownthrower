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
    'entry_points.task'  : 'brownthrower.task',
    'entry_points.event' : 'brownthrower.event',
    'manager.editor'     : 'nano',
    'database.url'       : 'postgresql://tallada:secret,@db01.pau.pic.es/test_tallada',
    'listing.limit'      : 50,
}

log = logging.getLogger('brownthrower.manager')
# TODO: Remove
logging.basicConfig(level=logging.DEBUG)

class Manager(cmd.Cmd):
    
    def __init__(self, *args, **kwargs):
        cmd.Cmd.__init__(self, *args, **kwargs)
        
        self._tasks   = {}
        self._subcmds = {}
        
        self.intro = "\nPAU DM Manager v0.1 is ready"
    
    def preloop(self):
        self._tasks = common.load_tasks(_CONFIG['entry_points.task'])
        
        from commands import Job  #@UnresolvedImport
        from commands import Task #@UnresolvedImport
        
        self._subcmds['job'] = Job(    tasks = self._tasks,
                                      editor = _CONFIG['manager.editor'],
                                       limit = _CONFIG['listing.limit'])
        self._subcmds['task'] = Task(  tasks = self._tasks)
    
    def do_help(self, line):
        items = line.strip().split()
        if items:
            subcmd = self._subcmds.get(items[0])
            if subcmd:
                return subcmd._help(items[1:])
        
        print textwrap.dedent("""\
        usage: <command> [options]
        
        Available commands:
            job     create, configure, submit and remove jobs
            quit    exit this program
            task    show all information about the available tasks
        """)
    
    def completedefault(self, text, line, begidx, endidx):
        items = line[:begidx].strip().split()
        if len(items) > 0:
            subcmd = self._subcmds.get(items[0])
            if subcmd:
                return subcmd._complete(text, items[1:])
    
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

if __name__ == '__main__':
    signal.signal(signal.SIGTERM, system_exit)
    
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger('sqlalchemy.engine').setLevel(logging.DEBUG)
    
    from pysrc import pydevd
    pydevd.settrace(suspend=False)
    
    #import rpdb
    #rpdb.Rpdb().set_trace()
    
    model.init(_CONFIG['database.url'])
    #model.init('sqlite:////tmp/manager.db')
    #model.Base.metadata.create_all()
    
    manager = Manager()
    manager.cmdloop()

