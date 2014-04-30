#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import cmd
import logging
import textwrap
# import signal
import sys
# import transaction

import brownthrower as bt
# from brownthrower.api.profile import settings
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm.session import sessionmaker
from tabulate import tabulate

log = logging.getLogger('brownthrower.manager')

class Manager(cmd.Cmd):
    """\
    usage: <command> [options]
    """
    
    def _parse_args(self, args = None):
        parser = argparse.ArgumentParser(prog='brownthrower', add_help=False)
        parser.add_argument('--database-url', '-u', default="sqlite:///", metavar='URL',
            help="use the settings in %(metavar)s to establish the database connection (default: '%(default)s')")
        parser.add_argument('--help', '-h', action='help',
            help='show this help message and exit')
        parser.add_argument('--version', '-v', action='version', 
            version='%%(prog)s %s' % bt.release.__version__)
        
        options = vars(parser.parse_args(args))
        
        return options
    
    def __init__(self, args):
        cmd.Cmd.__init__(self)
        
        options = self._parse_args(args)
        db_url = options.get('database_url')
        engine = bt.create_engine(db_url)
        
        self._session_maker = scoped_session(sessionmaker(engine))
        self._subcmds = {}
        
        self.prompt = '(brownthrower): '
    
    def preloop(self):
        from brownthrower.manager import commands
        
        self._subcmds['job']  = commands.Job( manager = self)
        self._subcmds['task'] = commands.Task(manager = self)
    
    @property
    def session_maker(self):
        return self._session_maker
    
    def do_help(self, line):
        items = line.strip().split()
        if items:
            subcmd = self._subcmds.get(items[0])
            if subcmd:
                return subcmd._help(items[1:])
        
        print textwrap.dedent(self.__doc__).strip()
        print "\nAvailable commands:"
        subcmds = {'quit' : 'Exit this program'}
        subcmds.update(self._subcmds)
        table = []
        for name in sorted(subcmds.keys()):
            if name == 'quit':
                table.append([" ", name, subcmds[name]])
            else:
                table.append([" ", name, textwrap.dedent(subcmds[name].__doc__).strip().split('\n')[2]])
        print tabulate(table, tablefmt="plain")
    
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
        return cmd.Cmd.postcmd(self, stop, line)
    
    def postloop(self):
        print

def main(args = None):
    if not args:
        args = sys.argv[1:]
    
    # TODO: Add debugging option
    #from pysrc import pydevd
    #pydevd.settrace()
    
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
    
    manager = Manager(args)
    try:
        manager.cmdloop()
    except KeyboardInterrupt:
        print

if __name__ == '__main__':
    main()
