#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import prettytable
import subprocess
import tempfile
import textwrap

from base import Command
from brownthrower import model
from brownthrower.manager import constants

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
        fh.write(task.get_config_template())
        fh.close()
        
        try:
            subprocess.check_call([self._editor, path])
            
            config = open(path, 'r').read()
            job = model.Job(name   = items[0],
                            status = constants.JobStatus.STASHED,
                            config = config)
            model.session.add(job)
            model.session.commit()
            print "A new job for task '%s' with id %d has been created." % (items[0], job.id)
        except:
            model.session.rollback()
            print "ERROR: The job could not be created."
    
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
            query = model.session.query(model.Job).filter(model.Job.id.in_(items))
        else:
            query = model.session.query(model.Job).limit(self._limit)
        
        try:
            jobs = query.all()
            model.session.commit()
            
            t = prettytable.PrettyTable(['id', 'event_id', 'name', 'status'])
            for job in jobs:
                t.add_row([job.id, job.event_id, job.name, job.status])
            
            if not jobs:
                print "WARNING: No jobs found matching the supplied criteria."
                return
            
            print t
        
        except:
            model.session.rollback()
            print "ERROR: Could not complete the query to the database."

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
        
        try:
            deleted = model.session.query(model.Job).filter(
                model.Job.id.in_(items),
                model.Job.status.in_([
                    constants.JobStatus.ABORTED,
                    constants.JobStatus.CANCELLED,
                    constants.JobStatus.CLEARED_FAILED,
                    constants.JobStatus.CLEARED_SUCCESS,
                    constants.JobStatus.OUTPUT_LOST,
                    constants.JobStatus.STEADY,
                    constants.JobStatus.STASHED,
                ])).delete(synchronize_session=False)
            model.session.commit()
            
            if deleted == len(items):
                print "All %d jobs have been successfully removed from the database." % deleted
            elif deleted > 0:
                print "WARNING: Only %d out of %d jobs have been successfully removed from the database." % (deleted, len(items))
            else: # deleted == 0
                print "WARNING: No jobs could be removed matching the supplied criteria."
        except:
            model.session.rollback()
            print "ERROR: Could not complete the query to the database."

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
        
        try:
            submitted = model.session.query(model.Job).filter(
                model.Job.id.in_(items),
                model.Job.status.in_([
                    constants.JobStatus.ABORTED,
                    constants.JobStatus.CANCELLED,
                    constants.JobStatus.STASHED,
                    constants.JobStatus.SUBMIT_FAIL,
                ])).update(
                    #TODO: Shall reset all the other fields
                    {'status' : constants.JobStatus.STEADY},
                synchronize_session = False \
            )
            model.session.commit()
            
            if submitted == len(items):
                print "All %d jobs have been successfully marked as ready for execution in the database." % submitted
            elif submitted > 0:
                print "WARNING: Only %d of %d jobs have been successfully marked as ready for execution." % (submitted, len(items))
            else: # submitted == 0
                print "ERROR: No jobs could be marked as ready matching the supplied criteria."
        except:
            model.session.rollback()
            print "ERROR: Could not complete the query to the database."

class JobReset(Command):
    
    def help(self, items):
        print textwrap.dedent("""\
        usage: job reset <id> ...
        
        Return the specified jobs to the stash for reconfiguring.
        """)
    
    def complete(self, text, items):
        return [text]
    
    def do(self, items):
        if not items:
            return self.help(items)
        
        try:
            resetted = model.session.query(model.Job).filter(
                model.Job.id.in_(items),
                model.Job.status.in_([
                    constants.JobStatus.ABORTED,
                    constants.JobStatus.CANCELLED,
                    constants.JobStatus.STEADY,
                    constants.JobStatus.SUBMIT_FAIL,
                ])).update(
                    #TODO: Shall reset all the other fields
                    {'status' : constants.JobStatus.STASHED},
                synchronize_session = False \
            )
            model.session.commit()
            
            if resetted == len(items):
                print "All %d jobs have been successfully returned to the stash." % resetted
            elif resetted > 0 and resetted < len(items):
                print "WARNING: Only %d of %d jobs have been successfully returned to the stash." % (resetted, len(items))
            else: # submitted == 0
                print "ERROR: No jobs could be returned to the stash matching the supplied criteria."
        except:
            model.session.rollback()
            print "ERROR: Could not complete the query to the database."

class JobAddParent(Command):
    
    def help(self, items):
        print textwrap.dedent("""\
        usage: job add_parent <id> <parent_id>
        
        Add a parent to the specified job.
        """)
    
    def complete(self, text, items):
        return [text]
    
    def do(self, items):
        if len(items) != 2:
            return self.help(items)
        
        try:
            jobs = model.session.query(model.Job).filter(
                model.Job.id.in_(items),
                model.Job.status.in_(
                    constants.JobStatus.manager_links_to(constants.JobStatus.STASHED)
                )
            )
            
            dependency = model.JobDependency(child_job_id = int(items[0]), parent_job_id = int(items[1]))
            model.session.add(dependency)
            model.session.commit()
            
            print "The dependency between jobs %d and %d has been succesfully established." % (int(items[0]), int(items[1]))
            
        except:
            model.session.rollback()
            print "ERROR: Could not complete the query to the database."

class JobAddChild(Command):
    
    def help(self, items):
        print textwrap.dedent("""\
        usage: job add_child <id> <child_id>
        
        Add a child to the specified job.
        """)
    
    def complete(self, text, items):
        return [text]
    
    def do(self, items):
        if len(items) != 2:
            return self.help(items)
        
        try:
            dependency = model.JobDependency(child_job_id = int(items[1]), parent_job_id = int(items[0]))
            model.session.add(dependency)
            model.session.commit()
            
            print "The dependency between jobs %d and %d has been succesfully established." % (int(items[0]), int(items[1]))
            
        except:
            model.session.rollback()
            print "ERROR: Could not complete the query to the database."

class JobCancel(Command):
    
    def help(self, items):
        print textwrap.dedent("""\
        usage: job cancel <id> ...
        
        Cancel the specified jobs as soon as possible.
        If the dispatcher has not already queued them, they are immediately cancelled.
        If the dispatcher has queued them, mark them to be cancelled as soon as possible. 
        """)
    
    def complete(self, text, items):
        return [text]
    
    def do(self, items):
        if not items:
            return self.help(items)
        
        try:
            cancel =  model.session.query(model.Job).filter(
                model.Job.id.in_(items),
                model.Job.status.in_([
                    constants.JobStatus.QUEUED,
                    constants.JobStatus.RUNNING,
                ])).update(
                    #TODO: Shall reset all the other fields
                    {'status' : constants.JobStatus.CANCEL},
                synchronize_session = False \
            )
            model.session.commit()
            
            if cancel == len(items):
                print "All %d jobs have been marked to be cancelled as soon as possible." % cancel
            elif cancel > 0:
                print "WARNING: Only %d out of %d jobs could be marked to be cancelled as soon as possible." % (cancel, len(items))
            else: # cancel == 0
                print "ERROR: No jobs could be cancelled matching the supplied criteria."
        except:
            model.session.rollback()
            print "ERROR: Could not complete the query to the database."
