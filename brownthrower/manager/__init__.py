#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import cmd
import logging
import textwrap
import signal
import sys
import transaction

from brownthrower import api, interface, model, release
from brownthrower.api.profile import settings

try:
    from logging import NullHandler
except ImportError:
    from logutils import NullHandler # @UnusedImport

log = logging.getLogger('brownthrower.manager')
log.addHandler(NullHandler())

class Manager(cmd.Cmd):
    
    def __init__(self, *args, **kwargs):
        cmd.Cmd.__init__(self, *args, **kwargs)
        
        self._dispatchers = {}
        self._subcmds     = {}
        
        self.prompt = '(brownthrower): '
    
    def preloop(self):
        # FIXME: Use api
        self._dispatchers = api._dispatchers
        
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
        print

def _parse_args(args = None):
    parser = argparse.ArgumentParser(prog='manager')
    parser.add_argument('-p', '--profile', const='default', nargs='?', default='default',
                        help="configuration profile for this session (default: 'default')")
    parser.add_argument('-u', '--database-url', default=argparse.SUPPRESS,
                        help='database connection settings')
    parser.add_argument('--editor', default=argparse.SUPPRESS,
                        help='command for editing text files')
    parser.add_argument('--pager', default=argparse.SUPPRESS,
                        help='command for displaying text files')
    parser.add_argument('--history-length', type=int, default=argparse.SUPPRESS,
                        help='number of history lines to preserve')
    parser.add_argument('-d', '--debug', const='pdb', nargs='?', default=argparse.SUPPRESS,
                        help="enable debugging framework (deactivated by default, 'pdb' if not specific framework requested)",
                        choices=['pydevd', 'ipdb', 'rpdb', 'pdb'])
    parser.add_argument('-v', '--version', action='version', 
                        version='%%(prog)s %s' % release.__version__)
    
    options = vars(parser.parse_args(args))
    
    return options

def main(args = None):
    if not args:
        args = sys.argv[1:]
    
    manager = Manager()
    print "brownthrower manager v{version} is loading...".format(
        version = release.__version__
    )
    options = _parse_args(args)
    api.init(options)
    
    try:
        manager.cmdloop()
    except KeyboardInterrupt:
        print

if __name__ == '__main__':
    main()
