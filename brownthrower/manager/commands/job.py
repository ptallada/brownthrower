#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import subprocess
import tempfile
import textwrap

from base import Command

class Job(Command):
    
    def help(self, items):
        print textwrap.dedent("""\
        usage: job <command> [options]
        
        Available commands:
            describe    list all available tasks
            create      create a single job
        """)
    
    def complete(self, text, items):
        available = self._subcmds.keys()
        
        return [command
                for command in available
                if command.startswith(text)]
    
    def do(self, items):
        self.help(items)

class JobDescribe(Command):
    
    def __init__(self, tasks, *args, **kwargs):
        super(JobDescribe, self).__init__(*args, **kwargs)
        self._tasks = tasks
    
    def help(self, items):
        print textwrap.dedent("""\
        usage: job describe [name]
        
        Show a list of all the tasks available in this environment.
        If a 'name' is supplied, show a detailed description of that task.
        """)
    
    def do(self, items):
        if not items:
            if len(self._tasks) == 0:
                print "There are no tasks currently registered in this environment."
                return
            
            max_task_len = max([len(task) for task in self._tasks])
            print
            print "This environment currently recognizes the following tasks:"
            print "=========================================================="
            for name, task in self._tasks.iteritems():
                print "{0:<{width}}    {1}".format(name, task.get_help()[0], width=max_task_len)
            print
            return
        
        # Show the details of one or more tasks
        for item in items:
            task = self._tasks.get(item)
            if task:
                help = task.get_help()
                header = "Details for the task '%s'" % item
                print
                print header
                print "=" * len(header)
                print help[0]
                print
                print help[1]
            else:
                print "The task '%s' is not currently available in this environment."
                print
    
    def complete(self, text, items):
        matching = set([key
                        for key in self._tasks.iterkeys()
                        if key.startswith(text)])
        
        return list(matching - set(items))

class JobCreate(Command):
    
    def __init__(self, tasks, editor, *args, **kwargs):
        super(JobCreate, self).__init__(*args, **kwargs)
        self._tasks  = tasks
        self._editor = editor
    
    def help(self, items):
        print textwrap.dedent("""\
        usage: job create <task>
        
        Create a new job of the specified task.
        """)
    
    def do(self, items):
        if not items or len(items) > 1:
            return self.help(items)
        
        task = self._tasks.get(item[0])
        if not task:
            print "The task '%s' is not currently available in this environment."
            return
        
        (fd, path) = tempfile.mkstemp(suffix, prefix, dir, text)
        fd.write(task.get_template())
        fd.close()
        
        try:
            subprocess.check_call([self._editor, path])
        except subprocess.CalledProcessError:
            print "Error: The job could not be created."
    
    def complete(self, text, items):
        if not items:
            matching = set([key
                            for key in self._tasks.iterkeys()
                            if key.startswith(text)])
            
            return list(matching - set(items))
