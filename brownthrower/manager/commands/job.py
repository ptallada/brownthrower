#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import prettytable
import subprocess
import tempfile
import textwrap

from base import Command
from brownthrower import model

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
        usage: job describe [name] ...
        
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

class JobCreate(Command):
    
    def __init__(self, tasks, editor, *args, **kwargs):
        super(JobCreate, self).__init__(*args, **kwargs)
        self._tasks   = tasks
        self._editor  = editor
    
    def help(self, items):
        print textwrap.dedent("""\
        usage: job create <task>
        
        Create a new job of the given task.
        """)
    
    def do(self, items):
        if not items or len(items) > 1:
            return self.help(items)
        
        task = self._tasks.get(items[0])
        if not task:
            print "The task '%s' is not currently available in this environment."
            return
        
        (fd, path) = tempfile.mkstemp()
        fh = os.fdopen(fd, 'w')
        fh.write(task.get_template())
        fh.close()
        
        try:
            subprocess.check_call([self._editor, path])
            
            config = open(path, 'r').read()
            job = model.Job(name   = items[0],
                            status = 'STASHED',
                            config = config)
            model.session.add(job)
            model.session.commit()
            print "A new job for task '%s' with id %d has been created." % (items[0], job.id)
        except subprocess.CalledProcessError:
            print "Error: The job could not be created."
    
    def complete(self, text, items):
        if not items:
            matching = set([key
                            for key in self._tasks.iterkeys()
                            if key.startswith(text)])
            
            return list(matching - set(items))

class JobShow(Command):
    
    def __init__(self, limit, *args, **kwargs):
        super(JobShow, self).__init__(*args, **kwargs)
        self._limit = limit
    
    def help(self, items):
        print textwrap.dedent("""\
        usage: job show [id] ...
        
        Show a list of all the jobs registered in the database.
        If any 'id' is supplied, show a detailed description of those jobs.
        """)
    
    def complete(self, text, items):
        return [text]
    
    def do(self, items):
        if items:
            jobs = model.session.query(model.Job).filter(model.Job.id.in_(items)).all()
        else:
            jobs = model.session.query(model.Job).limit(self._limit).all()
        
        model.session.commit()
        
        t = prettytable.PrettyTable(['id', 'event_id', 'name', 'status'])
        for job in jobs:
            t.add_row([job.id, job.event_id, job.name, job.status])
        
        if not jobs:
            print "WARNING: No jobs found matching the supplied criteria."
            return
        
        print t

class JobRemove(Command):
    
    def help(self, items):
        print textwrap.dedent("""\
        usage: job remove <id> ...
        
        Remove the job with the supplied id from the database.
        The job has to be in any of the final states.
        """)
    
    def complete(self, text, items):
        return [text]
    
    def do(self, items):
        if not items:
            return self.help(items)
        # TODO: Remove only in final states
        # TODO: comprovar en el queries amb cascada aquesta query no peta
        deleted = model.session.query(model.Job).filter(model.Job.id.in_(items)).delete(synchronize_session=False)
        model.session.commit()
        
        if deleted == len(items):
            print "%d jobs have been successfully removed from the database." % deleted
        elif deleted > 0 and deleted < len(items):
            print "WARNING: Only %d out of %d jobs have been successfully removed from the database." % (deleted, len(items))
        else: # deleted == 0
            print "ERROR: No jobs could be removed matching the supplied criteria."

class JobSubmit(Command):
    
    def help(self, items):
        print textwrap.dedent("""\
        usage: job submit <id> ...
        
        Mark the specified jobs as ready to be executed whenever there are resources available.
        """)
    
    def complete(self, text, items):
        return [text]
    
    def do(self, items):
        if not items:
            return self.help(items)
        
        # TODO: Submit only in initial state
        submitted = model.session.query(model.Job).filter(model.Job.id.in_(items)).update({'status' : 'READY'}, synchronize_session=False)
        
        if submitted == len(items):
            print "%d have been successfully marked as ready for execution in the database." % submitted
        elif submitted > 0 and submitted < len(items):
            print "WARNING: Only %d of %d jobs have been successfully marked as ready for execution." % (submitted, len(items))
        else: # submitted == 0
            print "ERROR: No jobs could be marked as ready matching the supplied criteria."
