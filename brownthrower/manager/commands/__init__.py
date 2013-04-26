#!/usr/bin/env python
# -*- coding: utf-8 -*-

import dispatcher
import job
import profile
import task
import textwrap

from base import Command
from brownthrower.profile import settings

class Job(Command):
    
    def __init__(self, *args, **kwargs):
        super(Job, self).__init__(*args, **kwargs)
        
        self.add_subcmd('cancel', job.JobCancel())
        self.add_subcmd('create', job.JobCreate())
        self.add_subcmd('edit',   job.JobEdit())
        self.add_subcmd('link',   job.JobLink())
        self.add_subcmd('list',   job.JobList())
        self.add_subcmd('output', job.JobOutput())
        self.add_subcmd('remove', job.JobRemove())
        self.add_subcmd('reset',  job.JobReset())
        self.add_subcmd('show',   job.JobShow())
        self.add_subcmd('submit', job.JobSubmit())
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
    
    def __init__(self, *args, **kwargs):
        super(Task, self).__init__(*args, **kwargs)
        
        self.add_subcmd('list',   task.TaskList())
        self.add_subcmd('schema', task.TaskSchema())
        self.add_subcmd('show',   task.TaskShow())
        self.add_subcmd('sample', task.TaskSample())
    
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

class Profile(Command):
    
    def __init__(self, *args, **kwargs):
        super(Profile, self).__init__(*args, **kwargs)
        
        self.add_subcmd('create',  profile.ProfileCreate())
        self.add_subcmd('default', profile.ProfileDefault())
        self.add_subcmd('edit',    profile.ProfileEdit())
        self.add_subcmd('list',    profile.ProfileList())
        self.add_subcmd('remove',  profile.ProfileRemove())
        self.add_subcmd('show',    profile.ProfileShow())
        self.add_subcmd('switch',  profile.ProfileSwitch())
    
    def help(self, items):
        print textwrap.dedent("""\
        usage: profile <command> [options]
        
        Available commands:
            create     create a new configuration profile
            default    set a configuration profile as the default
            edit       edit a configuration profile
            list       show available configuration profiles
            remove     delete a configuration profile
            show       display the settings of a configuration profile
            switch     apply the settings of a configuration profile
        """)
    
    def complete(self, text, items):
        available = self._subcmds.keys()
        
        return [command
                for command in available
                if command.startswith(text)]
    
    def do(self, items):
        self.help(items)
