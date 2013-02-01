#!/usr/bin/env python
# -*- coding: utf-8 -*-

from termcolor import cprint

def error(msg):
    cprint("ERROR: %s" % msg, color='red')

def warn(msg):
    cprint("WARNING: %s" % msg, color='yellow')

def success(msg):
    cprint("SUCCESS: %s" % msg, color='green')

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
        pass
    
    def complete(self, text, items):
        pass
    
    def do(self, items):
        pass
