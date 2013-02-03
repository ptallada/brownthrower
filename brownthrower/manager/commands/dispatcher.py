#!/usr/bin/env python
# -*- coding: utf-8 -*-

import prettytable
import textwrap

from base import Command, error, warn

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

#class TaskSchema(Command):
#    
#    _dataset_fn = {
#        'config' : lambda task: task.get_config_schema,
#        'input'  : lambda task: task.get_input_schema,
#        'output' : lambda task: task.get_output_schema,
#    }
#    
#    def __init__(self, tasks, *args, **kwargs):
#        super(TaskSchema, self).__init__(*args, **kwargs)
#        self._tasks = tasks
#    
#    def help(self, items):
#        print textwrap.dedent("""\
#        usage: task schema <dataset> <task>
#        
#        Show the schema of the specified dataset for a task.
#        Valid values for the dataset parameter are: 'input', 'output' and 'config'.
#        """)
#    
#    def complete(self, text, items):
#        if not items:
#            matching = [value
#                        for value in self._dataset_fn.keys()
#                        if value.startswith(text)]
#            return matching
#        
#        if (len(items) == 1) and (items[0] in self._dataset_fn):
#            matching = [key
#                        for key in self._tasks.iterkeys()
#                        if key.startswith(text)]
#            
#            return matching
#    
#    def do(self, items):
#        if (
#            (len(items) != 2) or
#            (items[0] not in self._dataset_fn) or
#            (items[1] not in self._tasks.keys())
#        ):
#            return self.help(items)
#        
#        task = self._tasks.get(items[1])
#        if not task:
#            error("The task '%s' is not currently available in this environment." % items[1])
#            return
#        
#        print self._dataset_fn[items[0]](task)()
#
#class TaskSample(Command):
#    
#    _dataset_fn = {
#        'config' : lambda task: task.get_config_sample,
#        'input'  : lambda task: task.get_input_sample,
#        'output' : lambda task: task.get_output_sample,
#    }
#        
#    def __init__(self, tasks, *args, **kwargs):
#        super(TaskSample, self).__init__(*args, **kwargs)
#        self._tasks = tasks
#    
#    def help(self, items):
#        print textwrap.dedent("""\
#        usage: task sample <dataset> <task>
#        
#        Show a sample of the specified dataset for a task.
#        Valid values for the dataset parameter are: 'input', 'output' and 'config'.
#        """)
#    
#    def complete(self, text, items):
#        if not items:
#            matching = [value
#                        for value in self._dataset_fn
#                        if value.startswith(text)]
#            return matching
#        
#        if (len(items) == 1) and (items[0] in self._dataset_fn):
#            matching = [key
#                        for key in self._tasks.iterkeys()
#                        if key.startswith(text)]
#            
#            return matching
#    
#    def do(self, items):
#        if (
#            (len(items) != 2) or
#            (items[0] not in self._dataset_fn.keys()) or
#            (items[1] not in self._tasks.keys())
#        ):
#            return self.help(items)
#        
#        task = self._tasks.get(items[1])
#        if not task:
#            error("The task '%s' is not currently available in this environment." % items[1])
#            return
#        
#        print self._dataset_fn[items[0]](task)()
