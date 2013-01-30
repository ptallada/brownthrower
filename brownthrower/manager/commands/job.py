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
    
    def complete(self, text, items):
        if not items:
            matching = [key
                        for key in self._tasks.iterkeys()
                        if key.startswith(text)]
            
            return matching
    
    def do(self, items):
        if len(items) != 1:
            return self.help(items)
        
        task = self._tasks.get(items[0])
        if not task:
            print "The task '%s' is not currently available in this environment."
            return
        
        try:
            job = model.Job(
                task   = items[0],
                status = constants.JobStatus.STASHED
            )
            model.session.add(job)
            model.session.commit()
            print "A new job for task '%s' with id %d has been created." % (items[0], job.id)
        except:
            model.session.rollback()
            print "ERROR: The job could not be created."

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
        # TODO: Show detailed information for a single job
        if items:
            query = model.session.query(model.Job).filter(model.Job.id.in_(items))
        else:
            query = model.session.query(model.Job).limit(self._limit)
        
        try:
            jobs = query.all()
            model.session.commit()
            
            t = prettytable.PrettyTable(['id', 'event_id', 'task', 'status'])
            for job in jobs:
                t.add_row([job.id, job.event_id, job.task, job.status])
            
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
        usage: job remove <id> { <id> } 
        
        Remove the jobs with the supplied ids from the database.
        The jobs must be in any of the final states.
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
                    constants.JobStatus.STASHED,
                ])
            ).delete(synchronize_session=False)
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
        usage: job submit <id> { <id> }
        
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
                model.Job.status == constants.JobStatus.STASHED,
            ).update(
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
        usage: job reset <id> { <id> }
        
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
                    constants.JobStatus.CLEARED_FAILED,
                ])
            ).update(
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

class JobLink(Command):
    
    def help(self, items):
        print textwrap.dedent("""\
        usage: job link <parent_id> <child_id>
        
        Establish a dependency between two jobs.
        """)
    
    def complete(self, text, items):
        return [text]
    
    def do(self, items):
        if len(items) != 2:
            return self.help(items)
        
        try:
            parent = model.session.query(model.Job).filter(
                model.Job.id == items[0],
                model.Job.status != constants.JobStatus.OUTPUT_LOST,
            ).with_lockmode('read').first()
            
            child = model.session.query(model.Job).filter(
                model.Job.id == items[1],
                model.Job.status == constants.JobStatus.STASHED
            ).with_lockmode('read').first()
            
            if not (parent and child):
                model.session.rollback()
                print "ERROR: It is not possible to establish a parent-child dependency between these jobs."
                return
            
            dependency = model.JobDependency(child_job_id = child.id, parent_job_id = parent.id)
            model.session.add(dependency)
            model.session.commit()
            
            print "The parent-child dependency has been succesfully established."
            
        except:
            model.session.rollback()
            print "ERROR: Could not complete the query to the database."

class JobUnlink(Command):
    
    def help(self, items):
        print textwrap.dedent("""\
        usage: job unlink <parent_id> <child_id>
        
        Remove the dependency between the specified jobs.
        """)
    
    def complete(self, text, items):
        return [text]
    
    def do(self, items):
        if len(items) != 2:
            return self.help(items)
        
        try:
            parent = model.session.query(model.Job).filter(
                model.Job.id == items[0],
                model.Job.status != constants.JobStatus.OUTPUT_LOST,
            ).with_lockmode('read').first()
            
            child = model.session.query(model.Job).filter(
                model.Job.id == items[1],
                model.Job.status == constants.JobStatus.STASHED
            ).with_lockmode('read').first()
            
            if not (parent and child):
                model.session.rollback()
                print "ERROR: It is not possible to remove the parent-child dependency."
                return
            
            deleted = model.session.query(model.JobDependency).filter_by(
                parent_job_id = parent.id,
                child_job_id  = child.id
            ).delete(synchronize_session=False)
            model.session.commit()
            
            if not deleted:
                print "ERROR: It is not possible to remove the parent-child dependency."
            else:
                print "The parent-child dependency has been succesfully removed."
        except:
            model.session.rollback()
            print "ERROR: Could not complete the query to the database."

class JobCancel(Command):
    
    def help(self, items):
        print textwrap.dedent("""\
        usage: job cancel <id> { <id> }
        
        Cancel the specified jobs as soon as possible.
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

class JobEdit(Command):
    
    _dataset_attr = {
        'config' : {
            'field'    : model.Job.config,
            'template' : lambda task: task.get_config_template,
            'check'    : lambda task: task.check_config,
        },
        'input'  : {
            'field'    : model.Job.input,
            'template' : lambda task: task.get_input_template,
            'check'    : lambda task: task.check_input,
        }
    }
    
    def __init__(self, tasks, editor, *args, **kwargs):
        super(JobEdit, self).__init__(*args, **kwargs)
        self._tasks   = tasks
        self._editor  = editor
    
    def help(self, items):
        print textwrap.dedent("""\
        usage: job edit <dataset> <id>
        
        Edit the specified dataset of a job.
        Valid values for the dataset parameter are: 'input' and 'config'.
        """)
    
    def complete(self, text, items):
        if not items:
            matching = [attr
                        for attr in self._dataset_attr.keys()
                        if attr.startswith(text)]
            return matching
    
    def do(self, items):
        if (
            (len(items) != 2) or
            (items[0] not in self._dataset_attr)
        ):
            return self.help(items)
        
        try:
            job = model.session.query(model.Job).filter_by(
                id = items[1],
                status = constants.JobStatus.STASHED,
            ).with_lockmode('update_nowait').one()
        except:
            model.session.rollback()
            print "ERROR: Could not lock the job for editting."
            return
        
        task = self._tasks.get(job.task)
        if not task:
            print "The task '%s' is not currently available in this environment."
            return
        
        try:
            field    = self._dataset_attr[items[0]]['field']
            template = self._dataset_attr[items[0]]['template'](task)()
            check    = self._dataset_attr[items[0]]['check'](task)
            
            current_value = getattr(job, field.key)
            if not current_value:
                current_value = template
            
            (fd, path) = tempfile.mkstemp()
            fh = os.fdopen(fd, 'w')
            fh.write(current_value)
            fh.close()
            
            subprocess.check_call([self._editor, path])
            
            new_value = open(path, 'r').read()
            
            try:
                check(new_value)
            except:
                model.session.rollback()
                print "ERROR: The supplied dataset is not valid."
                return
            
            setattr(job, field.key, new_value)
            model.session.commit()
            
            print "The job dataset has been successfully modified."
        except:
            model.session.rollback()
            print "ERROR: The job could not be editted."
