#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import prettytable
import textwrap

from ..base import Command, error, warn
from brownthrower import api, profile

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
        
        table = prettytable.PrettyTable(['name', 'description'], sortby='name')
        table.align = 'l'
        for name, task in api.get_tasks().iteritems():
            table.add_row([name, api.get_help(task)[0]])
        
        print table

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
            desc = api.get_help(task)
            print desc[0]
            print
            print desc[1]
        
        except BaseException as e:
            try:
                raise
            except KeyError:
                error("The task '%s' is not available in this environment." % items[0])
        finally:
            log.debug(e)

class TaskInput(Command):
    """\
    usage: task input <command> [options]
    """
    
    def __init__(self, *args, **kwargs):
        super(TaskInput, self).__init__(*args, **kwargs)
        
        from . import dataset
        
        self.add_subcmd('create',  dataset.TaskDatasetCreate( dataset = 'input',
                                                              profile = profile.input))
        self.add_subcmd('default', dataset.TaskDatasetDefault(dataset = 'input',
                                                              profile = profile.input))
        self.add_subcmd('edit',    dataset.TaskDatasetEdit(   dataset = 'input',
                                                              profile = profile.input,
                                                          validate_fn = api.validate_input))
        self.add_subcmd('list',    dataset.TaskDatasetList(   dataset = 'input',
                                                              profile = profile.input))
        self.add_subcmd('remove',  dataset.TaskDatasetRemove( dataset = 'input',
                                                              profile = profile.input))
        self.add_subcmd('sample',  dataset.TaskDatasetAttr(   dataset = 'input',
                                                              attr    = 'sample',
                                                              attr_fn = api.get_input_schema))
        self.add_subcmd('schema',  dataset.TaskDatasetAttr(   dataset = 'input',
                                                              attr    = 'schema',
                                                              attr_fn = api.get_input_schema))
        self.add_subcmd('show',    dataset.TaskDatasetShow(   dataset = 'input',
                                                              profile = profile.input))

class TaskConfig(Command):
    """\
    usage: task config <command> [options]
    """
    
    def __init__(self, *args, **kwargs):
        super(TaskConfig, self).__init__(*args, **kwargs)
        
        from . import dataset
        
        self.add_subcmd('create',  dataset.TaskDatasetCreate( dataset = 'config',
                                                              profile = profile.config))
        self.add_subcmd('default', dataset.TaskDatasetDefault(dataset = 'config',
                                                              profile = profile.config))
        self.add_subcmd('edit',    dataset.TaskDatasetEdit(   dataset = 'config',
                                                              profile = profile.config,
                                                          validate_fn = api.validate_config))
        self.add_subcmd('list',    dataset.TaskDatasetList(   dataset = 'config',
                                                              profile = profile.config))
        self.add_subcmd('remove',  dataset.TaskDatasetRemove( dataset = 'config',
                                                              profile = profile.config))
        self.add_subcmd('sample',  dataset.TaskDatasetAttr(   dataset = 'config',
                                                              attr    = 'sample',
                                                              attr_fn = api.get_config_schema))
        self.add_subcmd('schema',  dataset.TaskDatasetAttr(   dataset = 'config',
                                                              attr    = 'schema',
                                                              attr_fn = api.get_config_schema))
        self.add_subcmd('show',    dataset.TaskDatasetShow(   dataset = 'config',
                                                              profile = profile.config))

class TaskOutput(Command):
    """\
    usage: task output <command> [options]
    """
    
    def __init__(self, *args, **kwargs):
        super(TaskOutput, self).__init__(*args, **kwargs)
        
        from . import dataset
        
        self.add_subcmd('sample', dataset.TaskDatasetAttr(dataset = 'output',
                                                         attr    = 'sample',
                                                         attr_fn = api.get_output_sample))
        self.add_subcmd('schema', dataset.TaskDatasetAttr(dataset = 'output',
                                                         attr    = 'schema',
                                                         attr_fn = api.get_output_schema))
