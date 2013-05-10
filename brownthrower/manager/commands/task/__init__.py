#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import textwrap

from ..base import Command, error, warn
from brownthrower import api
from tabulate import tabulate

log = logging.getLogger('brownthrower.manager')

class TaskList(Command):
    """\
    usage: task list
    
    Show a list of all the tasks available in this environment.
    """
    
    def do(self, items):
        if len(items) > 0:
            return self.help(items)
        
        if len(api.get_tasks()) == 0:
            warn("There are no tasks currently registered in this environment.")
            return
        
        tasks = api.get_tasks()
        table = []
        for name in sorted(tasks.keys()):
            table.append([name, api.task.get_help(tasks[name])[0]])
        
        print tabulate(table, headers=['name', 'description'])

class TaskShow(Command):
    """\
    usage: task show <name>
    
    Show a detailed description of the task with the given name.
    """
    
    def complete(self, text, items):
        if not items:
            matching = [key
                        for key in api.get_tasks().iterkeys()
                        if key.startswith(text)]
            
            return matching
    
    def do(self, items):
        if len(items) != 1:
            return self.help(items)
        
        try:
            task = api.get_task(items[0])
            desc = api.task.get_help(task)
            print desc[0]
            print
            print desc[1]
        
        except Exception as e:
            try:
                raise
            except api.task.UnavailableException:
                error("The task '%s' is not available in this environment." % e.task)
            finally:
                log.debug(e)

class TaskInput(Command):
    """\
    usage: task input <command> [options]
    
    Create, edit and remove customized input datasets for the tasks.
    """
    
    def __init__(self, *args, **kwargs):
        super(TaskInput, self).__init__(*args, **kwargs)
        
        from . import dataset
        
        self.add_subcmd('create',  dataset.TaskDatasetCreate( dataset = 'input'))
        self.add_subcmd('default', dataset.TaskDatasetDefault(dataset = 'input'))
        self.add_subcmd('edit',    dataset.TaskDatasetEdit(   dataset = 'input'))
        self.add_subcmd('list',    dataset.TaskDatasetList(   dataset = 'input'))
        self.add_subcmd('remove',  dataset.TaskDatasetRemove( dataset = 'input'))
        self.add_subcmd('sample',  dataset.TaskDatasetAttr(   dataset = 'input',
                                                              attr    = 'sample'))
        self.add_subcmd('schema',  dataset.TaskDatasetAttr(   dataset = 'input',
                                                              attr    = 'schema'))
        self.add_subcmd('show',    dataset.TaskDatasetShow(   dataset = 'input'))

class TaskConfig(Command):
    """\
    usage: task config <command> [options]
    
    Create, edit and remove customized config datasets for the tasks.
    """
    
    def __init__(self, *args, **kwargs):
        super(TaskConfig, self).__init__(*args, **kwargs)
        
        from . import dataset
        
        self.add_subcmd('create',  dataset.TaskDatasetCreate( dataset = 'config'))
        self.add_subcmd('default', dataset.TaskDatasetDefault(dataset = 'config'))
        self.add_subcmd('edit',    dataset.TaskDatasetEdit(   dataset = 'config'))
        self.add_subcmd('list',    dataset.TaskDatasetList(   dataset = 'config'))
        self.add_subcmd('remove',  dataset.TaskDatasetRemove( dataset = 'config'))
        self.add_subcmd('sample',  dataset.TaskDatasetAttr(   dataset = 'config',
                                                              attr    = 'sample'))
        self.add_subcmd('schema',  dataset.TaskDatasetAttr(   dataset = 'config',
                                                              attr    = 'schema'))
        self.add_subcmd('show',    dataset.TaskDatasetShow(   dataset = 'config'))

class TaskOutput(Command):
    """\
    usage: task output <command> [options]
    
    Show detailed information about output datasets for the tasks.
    """
    
    def __init__(self, *args, **kwargs):
        super(TaskOutput, self).__init__(*args, **kwargs)
        
        from . import dataset
        
        self.add_subcmd('sample', dataset.TaskDatasetAttr(dataset = 'output',
                                                          attr    = 'sample'))
        self.add_subcmd('schema', dataset.TaskDatasetAttr(dataset = 'output',
                                                          attr    = 'schema'))
