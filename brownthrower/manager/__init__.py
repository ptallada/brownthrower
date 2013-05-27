#!/usr/bin/env python
# -*- coding: utf-8 -*-

import brownthrower.release
import cmd
import logging
import textwrap
import signal
import sys
import transaction

from brownthrower import api, interface, model
from brownthrower.api.profile import settings

log = logging.getLogger('brownthrower.manager')

class Manager(cmd.Cmd):
    
    def __init__(self, *args, **kwargs):
        cmd.Cmd.__init__(self, *args, **kwargs)
        
        self._dispatchers = {}
        self._subcmds     = {}
    
    def preloop(self):
        self._dispatchers = api.load_dispatchers()
        
        from brownthrower.manager.commands import Dispatcher, Job, Profile, Task
        
        self._subcmds['dispatcher'] = Dispatcher(dispatchers = self._dispatchers)
        self._subcmds['job']        = Job()
        self._subcmds['profile']    = Profile()
        self._subcmds['task']       = Task()
    
    def do_help(self, line):
        items = line.strip().split()
        if items:
            subcmd = self._subcmds.get(items[0])
            if subcmd:
                return subcmd._help(items[1:])
        
        print textwrap.dedent("""\
        usage: <command> [options]
        
        Available commands:
          dispatcher  show information about the available dispatchers
          job         create, configure, submit and remove jobs
          profile     create, edit and remove configuration profiles
          quit        exit this program
          task        show information about the available tasks
        """)
    
    def completedefault(self, text, line, begidx, endidx):
        items = line[:begidx].strip().split()
        if len(items) > 0:
            subcmd = self._subcmds.get(items[0])
            if subcmd:
                return subcmd._complete(text, items[1:])
    
    def do_dispatcher(self, line):
        items = line.strip().split()
        self._subcmds['dispatcher']._do(items)
    
    def do_job(self, line):
        items = line.strip().split()
        self._subcmds['job']._do(items)
    
    def do_profile(self, line):
        items = line.strip().split()
        self._subcmds['profile']._do(items)
    
    def do_task(self, line):
        items = line.strip().split()
        self._subcmds['task']._do(items)
    
    def do_quit(self, line):
        return self.do_EOF(line)
    
    def do_EOF(self, line):
        return True
    
    def postcmd(self, stop, line):
        transaction.abort()
        return cmd.Cmd.postcmd(self, stop, line)
    
    def postloop(self):
        try:
            import readline
            
            # FIXME: Register this function with atexit
            if api.profile.get_current():
                readline.write_history_file(api.profile.get_history_path(api.profile.get_current()))
        except Exception:
            pass
        
        print

def setup_debugger(dbg):
    if dbg == 'pydevd':
        from pysrc import pydevd
        pydevd.settrace(suspend=True)
    
    elif dbg == 'ipdb':
        import ipdb
        ipdb.set_trace()
    
    else:
        import pdb
        pdb.set_trace()

def setup_logging():
    try:
        from logging.config import dictConfig
    except ImportError:
        from logutils.dictconfig import dictConfig
    
    dictConfig(settings['logging'])
    
def system_exit(*args, **kwargs):
    sys.exit(1)

def main(args = None):
    signal.signal(signal.SIGTERM, system_exit)
    
    if not args:
        args = sys.argv[1:]
    
    manager = Manager()
    print "brownthrower manager v{version} is loading...".format(
        version = brownthrower.release.__version__
    )
    api.init(args)
    
    if settings['debug']:
        setup_debugger(settings['debug'])
    # TODO: logging should be configured at switch time
    setup_logging()
    manager.cmdloop()

if __name__ == '__main__':
    main()
