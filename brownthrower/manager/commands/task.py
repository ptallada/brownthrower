#!/usr/bin/env python
# -*- coding: utf-8 -*-

import prettytable
import textwrap

from base import Command, error, warn
from brownthrower import api

class TaskList(Command):
    def help(self, items):
        print textwrap.dedent("""\
        usage: task list
        
        Show a list of all the tasks available in this environment.
        """)
    
    def complete(self, text, items):
        pass
    
    def do(self, items):
        if len(items) > 0:
            return self.help(items)
        
        if len(api.get_tasks()) == 0:
            warn("There are no tasks currently registered in this environment.")
            return
        
        table = prettytable.PrettyTable(['name', 'description'], sortby='name')
        table.align = 'l'
        for name, task in api.get_tasks().iteritems():
            table.add_row([name, api.get_help(task)[0]])
        
        print table

class TaskShow(Command):
    def help(self, items):
        print textwrap.dedent("""\
        usage: task show <name>
        
        Show a detailed description of the specified task.
        """)
    
    def complete(self, text, items):
        if not items:
            matching = [key
                        for key in api.get_tasks().iterkeys()
                        if key.startswith(text)]
            
            return matching
    
    def do(self, items):
        if len(items) != 1:
            return self.help(items)
        
        task = api.get_task(items[0])
        if task:
            desc = api.get_help(task)
            print desc[0]
            print
            print desc[1]
        else:
            error("The task '%s' is not currently available in this environment." % items[0])

class TaskSchema(Command):
    
    _dataset_fn = {
        'config' : lambda task: api.get_config_schema(task),
        'input'  : lambda task: api.get_input_schema(task),
        'output' : lambda task: api.get_output_schema(task),
    }
    
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
                        for key in api.get_tasks().iterkeys()
                        if key.startswith(text)]
            
            return matching
    
    def do(self, items):
        if (
            (len(items) != 2) or
            (items[0] not in self._dataset_fn) or
            (items[1] not in api.get_tasks().keys())
        ):
            return self.help(items)
        
        task = api.get_task(items[1])
        if not task:
            error("The task '%s' is not currently available in this environment." % items[1])
            return
        
        print self._dataset_fn[items[0]](task)

class TaskSample(Command):
    
    _dataset_fn = {
        'config' : lambda task: api.get_config_sample(task),
        'input'  : lambda task: api.get_input_sample(task),
        'output' : lambda task: api.get_output_sample(task),
    }
    
    def help(self, items):
        print textwrap.dedent("""\
        usage: task sample <dataset> <task>
        
        Show a sample of the specified dataset for a task.
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
                        for key in api.get_tasks().iterkeys()
                        if key.startswith(text)]
            
            return matching
    
    def do(self, items):
        if (
            (len(items) != 2) or
            (items[0] not in self._dataset_fn.keys()) or
            (items[1] not in api.get_tasks().keys())
        ):
            return self.help(items)
        
        task = api.get_task(items[1])
        if not task:
            error("The task '%s' is not currently available in this environment." % items[1])
            return
        
        print self._dataset_fn[items[0]](task)
