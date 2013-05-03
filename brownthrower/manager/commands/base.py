#!/usr/bin/env python
# -*- coding: utf-8 -*-

import termcolor
import textwrap

from tabulate import tabulate

def error(msg):
    termcolor.cprint("ERROR: %s" % msg, color='red')

def warn(msg):
    termcolor.cprint("WARNING: %s" % msg, color='yellow')

def success(msg):
    termcolor.cprint("SUCCESS: %s" % msg, color='green')

def strong(msg):
    return termcolor.colored(msg, attrs=['bold'])

class Command(object):
    
    def __init__(self, *args, **kwargs):
        super(Command, self).__init__()
        
        self._subcmds = {}
    
    def add_subcmd(self, name, command):
        self._subcmds[name] = command
    
    def _help(self, items):
        if items:
            subcmd = self._subcmds.get(items[0])
            if subcmd:
                return subcmd._help(items[1:])
        return self.help(items)
    
    def _complete(self, text, items):
        if items:
            subcmd = self._subcmds.get(items[0])
            if subcmd:
                return subcmd._complete(text, items[1:])
        
        return self.complete(text, items)
    
    def _do(self, items):
        if items:
            subcmd = self._subcmds.get(items[0])
            if subcmd:
                return subcmd._do(items[1:])
        return self.do(items)
    
    def help(self, items):
        print textwrap.dedent(self.__doc__).strip()
        if self._subcmds:
            print "\nAvailable commands:"
            print tabulate([
                (" ", name, textwrap.dedent(self._subcmds[name].__doc__).strip().split('\n')[2])
                for name in sorted(self._subcmds.keys())
            ], tablefmt="plain")
    
    def complete(self, text, items):
        # Autocomplete with subcommands by default
        available = self._subcmds.keys()
        
        return [command
                for command in available
                if command.startswith(text)]
    
    def do(self, items):
        # Show usage by default
        self.help(items)
