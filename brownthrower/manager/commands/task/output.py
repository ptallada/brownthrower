#!/usr/bin/env python
# -*- coding: utf-8 -*-

import subprocess
import textwrap

from ..base import Command, error
from brownthrower import api

class TaskOutputSchema(Command):
    
    def help(self, items):
        print textwrap.dedent("""\
        usage: task output schema <task>
        
        Show the schema of the output dataset for a task.
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
        if not task:
            error("The task '%s' is not currently available in this environment." % items[0])
            return
        
        viewer = subprocess.Popen(['pager'], stdin=subprocess.PIPE)
        viewer.communicate(input=api.get_output_schema(task))

class TaskOutputSample(Command):
    
    def help(self, items):
        print textwrap.dedent("""\
        usage: task output sample <task>
        
        Show a sample of the output dataset for a task.
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
        if not task:
            error("The task '%s' is not currently available in this environment." % items[0])
            return
        
        viewer = subprocess.Popen(['pager'], stdin=subprocess.PIPE)
        viewer.communicate(input=api.get_output_sample(task))
