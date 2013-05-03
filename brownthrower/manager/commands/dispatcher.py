#!/usr/bin/env python
# -*- coding: utf-8 -*-

import prettytable
import textwrap

from .base import Command, error, warn

class DispatcherList(Command):
    
    def __init__(self, dispatchers, *args, **kwargs):
        super(DispatcherList, self).__init__(*args, **kwargs)
        self._dispatchers = dispatchers
    
    def help(self, items):
        print textwrap.dedent("""\
        usage: dispatcher list
        
        Show a list of all the dispatchers available in this environment.
        """)
    
    def complete(self, text, items):
        pass
    
    def do(self, items):
        if len(items) > 0:
            return self.help(items)
        
        if len(self._dispatchers) == 0:
            warn("There are no dispatchers currently registered in this environment.")
            return
        
        table = prettytable.PrettyTable(['name', 'description'], sortby='name')
        table.align = 'l'
        for name, dispatcher in self._dispatchers.iteritems():
            table.add_row([name, dispatcher.get_help()[0]])
        
        print table

class DispatcherShow(Command):
    
    def __init__(self, dispatchers, *args, **kwargs):
        super(DispatcherShow, self).__init__(*args, **kwargs)
        self._dispatchers = dispatchers
    
    def help(self, items):
        print textwrap.dedent("""\
        usage: dispatcher show <name>
        
        Show a detailed description of the specified dispatcher.
        """)
    
    def complete(self, text, items):
        if not items:
            matching = [key
                        for key in self._dispatchers.iterkeys()
                        if key.startswith(text)]
            
            return matching
    
    def do(self, items):
        if len(items) != 1:
            return self.help(items)
        
        dispatcher = self._dispatchers.get(items[0])
        if dispatcher:
            desc = dispatcher.get_help()
            print desc[0]
            print desc[1]
        else:
            error("The dispatcher '%s' is not currently available in this environment." % items[0])

class DispatcherRun(Command):
    
    def __init__(self, dispatchers, *args, **kwargs):
        super(DispatcherRun, self).__init__(*args, **kwargs)
        self._dispatchers = dispatchers
    
    def help(self, items):
        print textwrap.dedent("""\
        usage: dispatcher run <name>
        
        Run the specified dispatcher until it is interrupted or there are no
        more jobs to be executed..
        """)
    
    def complete(self, text, items):
        if not items:
            matching = [key
                        for key in self._dispatchers.iterkeys()
                        if key.startswith(text)]
            
            return matching
    
    def do(self, items):
        if len(items) != 1:
            return self.help(items)
        
        dispatcher = self._dispatchers.get(items[0])
        if dispatcher:
            dispatcher().run()
        else:
            error("The dispatcher '%s' is not currently available in this environment." % items[0])
