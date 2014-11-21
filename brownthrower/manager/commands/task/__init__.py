#!/usr/bin/env python
# -*- coding: utf-8 -*-

import brownthrower as bt
import logging
import textwrap

from ..base import Command, error, warn
from tabulate import tabulate

try:
    from logging import NullHandler
except ImportError:
    from logutils import NullHandler

log = logging.getLogger('brownthrower.manager')
log.addHandler(NullHandler())

class TaskList(Command):
    """\
    usage: task list
    
    Show a list of all the tasks available in this environment.
    """
    
    def do(self, items):
        if len(items) > 0:
            return self.help(items)
        
        if len(bt.tasks) == 0:
            warn("There are no tasks currently registered in this environment.")
            return
        
        table = []
        for name in sorted(bt.tasks.keys()):
            try:
                table.append([name, bt.tasks[name].__module__])
            except KeyError:
                pass
        
        print tabulate(table, headers=['name', 'module'])

class TaskShow(Command):
    """\
    usage: task show <name>
    
    Show a detailed description of the task with the given name.
    """
    
    def complete(self, text, items):
        if not items:
            matching = [key
                        for key in bt.tasks.iterkeys()
                        if key.startswith(text)]
            
            return matching
    
    def do(self, items):
        if len(items) != 1:
            return self.help(items)
        
        try:
            task = bt.tasks[items[0]]
            print task.summary
            print
            print task.description
        
        except KeyError as e:
            error("The task '%s' is not available in this environment." % e.task)
            log.debug(e)

# class TaskInput(Command):
#     """\
#     usage: task input <command> [options]
#     
#     Create, edit and remove customized input datasets for the tasks.
#     """
#     
#     def __init__(self, *args, **kwargs):
#         super(TaskInput, self).__init__(*args, **kwargs)
#         
#         from . import dataset
#         
#         self.add_subcmd('create',  dataset.TaskDatasetCreate( dataset = 'input'))
#         self.add_subcmd('default', dataset.TaskDatasetDefault(dataset = 'input'))
#         self.add_subcmd('edit',    dataset.TaskDatasetEdit(   dataset = 'input'))
#         self.add_subcmd('list',    dataset.TaskDatasetList(   dataset = 'input'))
#         self.add_subcmd('remove',  dataset.TaskDatasetRemove( dataset = 'input'))
#         self.add_subcmd('sample',  dataset.TaskDatasetAttr(   dataset = 'input',
#                                                               attr    = 'sample'))
#         self.add_subcmd('schema',  dataset.TaskDatasetAttr(   dataset = 'input',
#                                                               attr    = 'schema'))
#         self.add_subcmd('show',    dataset.TaskDatasetShow(   dataset = 'input'))
# 
# class TaskConfig(Command):
#     """\
#     usage: task config <command> [options]
#     
#     Create, edit and remove customized config datasets for the tasks.
#     """
#     
#     def __init__(self, *args, **kwargs):
#         super(TaskConfig, self).__init__(*args, **kwargs)
#         
#         from . import dataset
#         
#         self.add_subcmd('create',  dataset.TaskDatasetCreate( dataset = 'config'))
#         self.add_subcmd('default', dataset.TaskDatasetDefault(dataset = 'config'))
#         self.add_subcmd('edit',    dataset.TaskDatasetEdit(   dataset = 'config'))
#         self.add_subcmd('list',    dataset.TaskDatasetList(   dataset = 'config'))
#         self.add_subcmd('remove',  dataset.TaskDatasetRemove( dataset = 'config'))
#         self.add_subcmd('sample',  dataset.TaskDatasetAttr(   dataset = 'config',
#                                                               attr    = 'sample'))
#         self.add_subcmd('schema',  dataset.TaskDatasetAttr(   dataset = 'config',
#                                                               attr    = 'schema'))
#         self.add_subcmd('show',    dataset.TaskDatasetShow(   dataset = 'config'))
# 
# class TaskOutput(Command):
#     """\
#     usage: task output <command> [options]
#     
#     Show detailed information about output datasets for the tasks.
#     """
#     
#     def __init__(self, *args, **kwargs):
#         super(TaskOutput, self).__init__(*args, **kwargs)
#         
#         from . import dataset
#         
#         self.add_subcmd('sample', dataset.TaskDatasetAttr(dataset = 'output',
#                                                           attr    = 'sample'))
#         self.add_subcmd('schema', dataset.TaskDatasetAttr(dataset = 'output',
#                                                           attr    = 'schema'))
