#!/usr/bin/env python
# -*- coding: utf-8 -*-

import prettytable
import textwrap

from base import Command

class TaskShow(Command):
    
    def __init__(self, tasks, *args, **kwargs):
        super(TaskShow, self).__init__(*args, **kwargs)
        self._tasks = tasks
    
    def help(self, items):
        print textwrap.dedent("""\
        usage: task show [name]
        
        Show a list of all the tasks available in this environment.
        If a 'name' is supplied, show a detailed description of that task.
        """)
    
    def complete(self, text, items):
        if not items:
            matching = [key
                        for key in self._tasks.iterkeys()
                        if key.startswith(text)]
            
            return matching
    
    def do(self, items):
        if len(items) > 1:
            return self.help(items)
        
        if not items:
            if len(self._tasks) == 0:
                print "There are no tasks currently registered in this environment."
                return
            
            t = prettytable.PrettyTable(['name', 'description'])
            for name, task in self._tasks.iteritems():
                t.add_row([name, task.get_help()[0]])
            
            print t
            return
        
        # Show the details of one or more tasks
        task = self._tasks.get(items[0])
        if task:
            desc = task.get_help()
            print desc[0]
            print
            print desc[1]
        else:
            print "ERROR: The task '%s' is not currently available in this environment."

class TaskSchema(Command):
    
    _dataset_fn = {
        'config' : lambda task: task.get_config_schema,
        'input'  : lambda task: task.get_input_schema,
        'output' : lambda task: task.get_output_schema,
    }
    
    def __init__(self, tasks, *args, **kwargs):
        super(TaskSchema, self).__init__(*args, **kwargs)
        self._tasks = tasks
    
    def help(self, items):
        print textwrap.dedent("""\
        usage: task schema <dataset> <task>
        
        Show the schema of the specified dataset for a task.
        Valid values for the dataset parameter are: 'input', 'output' and 'config'.
        """)
    
    def complete(self, text, items):
        if not items:
            matching = [value
                        for value in self._dataset_fn.keys()
                        if value.startswith(text)]
            return matching
        
        if (len(items) == 1) and (items[0] in self._dataset_fn):
            matching = [key
                        for key in self._tasks.iterkeys()
                        if key.startswith(text)]
            
            return matching
    
    def do(self, items):
        if (
            (len(items) != 2) or
            (items[0] not in self._dataset_fn) or
            (items[1] not in self._tasks.keys())
        ):
            return self.help(items)
        
        task = self._tasks.get(items[1])
        if not task:
            print "ERROR: The task '%s' is not currently available in this environment."
            return
        
        try:
            print self._dataset_fn[items[0]](task)()
        except:
            print "ERROR: Could not retrieve the requested schema."

class TaskTemplate(Command):
    
    _dataset_fn = {
        'config' : lambda task: task.get_config_template,
        'input'  : lambda task: task.get_input_template,
        'output' : lambda task: task.get_output_template,
    }
        
    def __init__(self, tasks, *args, **kwargs):
        super(TaskTemplate, self).__init__(*args, **kwargs)
        self._tasks = tasks
    
    def help(self, items):
        print textwrap.dedent("""\
        usage: task template <dataset> <task>
        
        Show a template of the specified dataset for a task.
        Valid values for the dataset parameter are: 'input', 'output' and 'config'.
        """)
    
    def complete(self, text, items):
        if not items:
            matching = [value
                        for value in self._dataset_fn
                        if value.startswith(text)]
            return matching
        
        if (len(items) == 1) and (items[0] in self._dataset_fn):
            matching = [key
                        for key in self._tasks.iterkeys()
                        if key.startswith(text)]
            
            return matching
    
    def do(self, items):
        if (
            (len(items) != 2) or
            (items[0] not in self._dataset_fn.keys()) or
            (items[1] not in self._tasks.keys())
        ):
            return self.help(items)
        
        task = self._tasks.get(items[1])
        if not task:
            print "ERROR: The task '%s' is not currently available in this environment."
            return
        
        try:
            print self._dataset_fn[items[0]](task)()
        except:
            print "ERROR: Could not retrieve the requested template."