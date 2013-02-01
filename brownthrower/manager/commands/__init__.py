#!/usr/bin/env python
# -*- coding: utf-8 -*-

import textwrap

import job
import task

from base import Command

class Job(Command):
    
    def __init__(self, tasks, editor, limit, *args, **kwargs):
        super(Job, self).__init__(*args, **kwargs)
        
        self.add_subcmd('cancel', job.JobCancel())
        self.add_subcmd('create', job.JobCreate(  tasks = tasks,
                                                 editor = editor))
        self.add_subcmd('edit',   job.JobEdit(    tasks = tasks,
                                                 editor = editor))
        self.add_subcmd('link',   job.JobLink())
        self.add_subcmd('remove', job.JobRemove())
        self.add_subcmd('reset',  job.JobReset())
        self.add_subcmd('show',   job.JobShow(    limit = limit))
        self.add_subcmd('submit', job.JobSubmit(  tasks = tasks))
        self.add_subcmd('unlink', job.JobUnlink())

    
    def help(self, items):
        print textwrap.dedent("""\
        usage: job <command> [options]
        
        Available commands:
            cancel    cancel a job o mark it to be cancelled
            create    create and configure a job
            edit      edit the value of a dataset
            link      establish a dependency between two jobs
            remove    delete a job which is in a final state
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
        
        self.add_subcmd('schema', task.TaskSchema(tasks = tasks))
        self.add_subcmd('show',   task.TaskShow(  tasks = tasks))
        self.add_subcmd('sample', task.TaskSample(tasks = tasks))
    
    def help(self, items):
        print textwrap.dedent("""\
        usage: task <command> [options]
        
        Available commands:
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
