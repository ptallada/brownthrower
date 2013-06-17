#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import traceback

from .base import Command, error, warn
from tabulate import tabulate

try:
    from logging import NullHandler
except ImportError:
    from logutils import NullHandler # @UnusedImport

log = logging.getLogger('brownthrower.manager')

class DispatcherList(Command):
    """\
    usage: dispatcher list
        
    Show a list of all the dispatchers available in this environment.
    """
    
    def __init__(self, dispatchers, *args, **kwargs):
        super(DispatcherList, self).__init__(*args, **kwargs)
        self._dispatchers = dispatchers
    
    def do(self, items):
        if len(items) > 0:
            return self.help(items)
        
        if len(self._dispatchers) == 0:
            warn("There are no dispatchers currently registered in this environment.")
            return
        
        table = []
        for name in sorted(self._dispatchers.keys()):
            table.append([name, self._dispatchers[name].get_help()[0]])
        
        print tabulate(table, headers=['name', 'description'])

class DispatcherShow(Command):
    """\
    usage: dispatcher show <name>
    
    Show a detailed description of the dispatcher with the given name.
    """
        
    def __init__(self, dispatchers, *args, **kwargs):
        super(DispatcherShow, self).__init__(*args, **kwargs)
        self._dispatchers = dispatchers
    
    def complete(self, text, items):
        if not items:
            matching = [key
                        for key in self._dispatchers.iterkeys()
                        if key.startswith(text)]
            
            return matching
    
    def do(self, items):
        if len(items) != 1:
            return self.help(items)
        
        #FIXME: Move to api
        dispatcher = self._dispatchers.get(items[0])
        if dispatcher:
            desc = dispatcher.get_help()
            print desc[0]
            print desc[1]
        else:
            error("The dispatcher '%s' is not currently available in this environment." % items[0])

class DispatcherRun(Command):
    """\
    usage: dispatcher run <name> [arguments]
        
    Run the specified dispatcher until it is interrupted or there are no more jobs to be executed.
    Each dispatcher has its own set of accepted arguments, please refer to its own documentation for help.
    """
    
    def __init__(self, dispatchers, *args, **kwargs):
        super(DispatcherRun, self).__init__(*args, **kwargs)
        self._dispatchers = dispatchers
    
    def complete(self, text, items):
        if not items:
            matching = [key
                        for key in self._dispatchers.iterkeys()
                        if key.startswith(text)]
            
            return matching
    
    def do(self, items):
        if len(items) < 1:
            return self.help(items)
        
        # FIXME: Rewrite as the job
        dispatcher = self._dispatchers.get(items[0])
        if not dispatcher:
            error("The dispatcher '%s' is not currently available in this environment." % items[0])
            return
        
        try:
            dispatcher().run(*items[1:])
        except BaseException:
            try:
                raise
            except Exception as e:
                error("The dispatcher threw an error: %s" % e)
            except KeyboardInterrupt:
                pass
            finally:
                ex = traceback.format_exc()
                log.debug(ex)
