#!/usr/bin/env python
# -*- coding: utf-8 -*-

import textwrap

import chain
import dispatcher
import job
import task

from base import Command

class Job(Command):
    
    def __init__(self, tasks, editor, viewer, limit, *args, **kwargs):
        super(Job, self).__init__(*args, **kwargs)
        
        self.add_subcmd('cancel', job.JobCancel())
        self.add_subcmd('create', job.JobCreate(  tasks = tasks,
                                                 editor = editor))
        self.add_subcmd('edit',   job.JobEdit(    tasks = tasks,
                                                 editor = editor))
        self.add_subcmd('link',   job.JobLink())
        self.add_subcmd('list',   job.JobList(    limit = limit))
        self.add_subcmd('output', job.JobOutput( viewer = viewer))
        self.add_subcmd('remove', job.JobRemove())
        self.add_subcmd('reset',  job.JobReset())
        self.add_subcmd('show',   job.JobShow())
        self.add_subcmd('submit', job.JobSubmit(  tasks = tasks))
        self.add_subcmd('unlink', job.JobUnlink())
    
    def help(self, items):
        print textwrap.dedent("""\
        usage: job <command> [options]
        
        Available commands:
            cancel    cancel a job as soon as possible
            create    create a new job of a task
            edit      edit the value of a dataset
            link      establish a dependency between two jobs
            list      list all registered jobs
            output    show the output of a job
            remove    delete a job
            reset     return a job to the stash
            show      show detailed information of a job
            submit    mark a job as ready to be executed
            unlink    remove a dependency between two jobs
        """)
    
    def complete(self, text, items):
        available = self._subcmds.keys()
        
        return [command
                for command in available
                if command.startswith(text)]
    
    def do(self, items):
        self.help(items)

class Task(Command):
    
    def __init__(self, tasks, *args, **kwargs):
        super(Task, self).__init__(*args, **kwargs)
        
        self.add_subcmd('list',   task.TaskList(  tasks = tasks))
        self.add_subcmd('schema', task.TaskSchema(tasks = tasks))
        self.add_subcmd('show',   task.TaskShow(  tasks = tasks))
        self.add_subcmd('sample', task.TaskSample(tasks = tasks))
    
    def help(self, items):
        print textwrap.dedent("""\
        usage: task <command> [options]
        
        Available commands:
            list      list all available tasks
            schema    show the formal schema of a task dataset
            show      show detailed information for a task
            sample    show a sample of a task dataset
        """)
    
    def complete(self, text, items):
        available = self._subcmds.keys()
        
        return [command
                for command in available
                if command.startswith(text)]
    
    def do(self, items):
        self.help(items)

class Dispatcher(Command):
    
    def __init__(self, dispatchers, *args, **kwargs):
        super(Dispatcher, self).__init__(*args, **kwargs)
        
        self.add_subcmd('list', dispatcher.DispatcherList(dispatchers = dispatchers))
        self.add_subcmd('run',  dispatcher.DispatcherRun( dispatchers = dispatchers))
        self.add_subcmd('show', dispatcher.DispatcherShow(dispatchers = dispatchers))
    
    def help(self, items):
        print textwrap.dedent("""\
        usage: dispatcher <command> [options]
        
        Available commands:
            list        show detailed information for a dispatcher
            run         run a dispatcher to execute jobs
            show        list available dispatchers
        """)
    
    def complete(self, text, items):
        available = self._subcmds.keys()
        
        return [command
                for command in available
                if command.startswith(text)]
    
    def do(self, items):
        self.help(items)

class Chain(Command):
    
    def __init__(self, chains, *args, **kwargs):
        super(Chain, self).__init__(*args, **kwargs)
        
        self.add_subcmd('list',   chain.ChainList(  chains = chains))
        self.add_subcmd('schema', chain.ChainSchema(chains = chains))
        self.add_subcmd('show',   chain.ChainShow(  chains = chains))
        self.add_subcmd('sample', chain.ChainSample(chains = chains))
    
    def help(self, items):
        print textwrap.dedent("""\
        usage: chain <command> [options]
        
        Available commands:
            list      list all available chains
            schema    show the formal schema of a chain dataset
            show      show detailed information for a chain
            sample    show a sample of a chain dataset
        """)
    
    def complete(self, text, items):
        available = self._subcmds.keys()
        
        return [command
                for command in available
                if command.startswith(text)]
    
    def do(self, items):
        self.help(items)
