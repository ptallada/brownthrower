#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import prettytable
import textwrap

from . import input, config
from ..base import Command, error, warn
from brownthrower import api

log = logging.getLogger('brownthrower.manager')

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

# FIXME: Refer els commands per q el help sigui automàtic, entre d'altres :)
class TaskInput(Command):
    
    def __init__(self, *args, **kwargs):
        super(TaskInput, self).__init__(*args, **kwargs)
        
        self.add_subcmd('create',  input.TaskInputCreate())
        self.add_subcmd('default', input.TaskInputDefault())
        self.add_subcmd('edit',    input.TaskInputEdit())
        self.add_subcmd('list',    input.TaskInputList())
        self.add_subcmd('remove',  input.TaskInputRemove())
        self.add_subcmd('sample',  input.TaskInputSample())
        self.add_subcmd('schema',  input.TaskInputSchema())
        self.add_subcmd('show',    input.TaskInputShow())
    
    def help(self, items):
        print textwrap.dedent("""\
        usage: task input <command> [options]
        
        Available commands:
            create     add a new dataset
            default    set a given dataset as the default
            edit       edit a dataset
            list       list available datasets
            remove     delete a dataset
            sample     show a sample for this dataset
            schema     show this dataset schema
            show       display a dataset contents
        """)
    
    def complete(self, text, items):
        available = self._subcmds.keys()
        
        return [command
                for command in available
                if command.startswith(text)]
    
    def do(self, items):
        self.help(items)

class TaskConfig(Command):
    
    def __init__(self, *args, **kwargs):
        super(TaskConfig, self).__init__(*args, **kwargs)
        
        self.add_subcmd('create',  config.TaskConfigCreate())
        self.add_subcmd('default', config.TaskConfigDefault())
        self.add_subcmd('edit',    config.TaskConfigEdit())
        self.add_subcmd('list',    config.TaskConfigList())
        self.add_subcmd('remove',  config.TaskConfigRemove())
        self.add_subcmd('sample',  config.TaskConfigSample())
        self.add_subcmd('schema',  config.TaskConfigSchema())
        self.add_subcmd('show',    config.TaskConfigShow())
    
    def help(self, items):
        print textwrap.dedent("""\
        usage: task config <command> [options]
        
        Available commands:
            create     add a new dataset
            default    set a given dataset as the default
            edit       edit a dataset
            list       list available datasets
            remove     delete a dataset
            sample     show a sample for this dataset
            schema     show this dataset schema
            show       display a dataset contents
        """)
    
    def complete(self, text, items):
        available = self._subcmds.keys()
        
        return [command
                for command in available
                if command.startswith(text)]
    
    def do(self, items):
        self.help(items)


class TaskOutput(Command):
    
    def __init__(self, *args, **kwargs):
        super(TaskOutput, self).__init__(*args, **kwargs)
        
        from . import output
        
        self.add_subcmd('schema', output.TaskOutputSchema())
        self.add_subcmd('sample', output.TaskOutputSample())
    
    def help(self, items):
        print textwrap.dedent("""\
        usage: task output <command> [options]
        
        Available commands:
            sample     show a sample for this dataset
            schema     show this dataset schema
        """)
    
    def complete(self, text, items):
        available = self._subcmds.keys()
        
        return [command
                for command in available
                if command.startswith(text)]
    
    def do(self, items):
        self.help(items)
