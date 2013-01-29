#!/usr/bin/env python
# -*- coding: utf-8 -*-

import textwrap

import job
import task

from base import Command

class Job(Command):
    
    def __init__(self, tasks, editor, limit, *args, **kwargs):
        super(Job, self).__init__(*args, **kwargs)
        
        self.add_subcmd('add_child',  job.JobAddChild())
        self.add_subcmd('add_parent', job.JobAddParent())
        self.add_subcmd('create',     job.JobCreate(  tasks = tasks,
                                                 editor = editor))
        self.add_subcmd('show',       job.JobShow(    limit = limit))
        self.add_subcmd('remove',     job.JobRemove())
        self.add_subcmd('submit',     job.JobSubmit())
        self.add_subcmd('cancel',     job.JobCancel())
        self.add_subcmd('reset',      job.JobReset())
    
    def help(self, items):
        print textwrap.dedent("""\
        usage: job <command> [options]
        
        Available commands:
            add_child     add a child to a job
            add_parent    add a parent to a job
            cancel        cancel a job o mark it to be cancelled
            create        create and configure a job
            remove        delete a job which is in a final state
            reset         return a job to the stash
            show          show detailed information for a job
            submit        mark a job as ready to be executed
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
        
        self.add_subcmd('show', task.TaskShow(tasks = tasks))
    
    def help(self, items):
        print textwrap.dedent("""\
        usage: task <command> [options]
        
        Available commands:
            show        show detailed information for a task
        """)
    
    def complete(self, text, items):
        available = self._subcmds.keys()
        
        return [command
                for command in available
                if command.startswith(text)]
    
    def do(self, items):
        self.help(items)
