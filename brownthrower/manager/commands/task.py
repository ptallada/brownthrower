#!/usr/bin/env python
# -*- coding: utf-8 -*-

import prettytable
import textwrap

from base import Command

class TaskShow(Command):
    
    def __init__(self, tasks, *args, **kwargs):
        super(TaskShow, self).__init__(*args, **kwargs)
        self._tasks = tasks
    
    def help(self, items):
        print textwrap.dedent("""\
        usage: task show [name] ...
        
        Show a list of all the tasks available in this environment.
        If a 'name' is supplied, show a detailed description of that task.
        """)
    
    def do(self, items):
        if not items:
            if len(self._tasks) == 0:
                print "There are no tasks currently registered in this environment."
                return
            
            t = prettytable.PrettyTable(['name', 'description'])
            for name, task in self._tasks.iteritems():
                t.add_row([name, task.get_help()[0]])
            
            print t
            return
        
        # Show the details of one or more tasks
        for item in items:
            task = self._tasks.get(item)
            if task:
                desc = task.get_help()
                header = "Details for the task '%s'" % item
                print
                print header
                print "=" * len(header)
                print desc[0]
                print
                print desc[1]
            else:
                print "The task '%s' is not currently available in this environment."
                print
    
    def complete(self, text, items):
        matching = set([key
                        for key in self._tasks.iterkeys()
                        if key.startswith(text)])
        
        return list(matching - set(items))
