#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import prettytable
import subprocess
import tempfile
import textwrap

from base import Command
from brownthrower import interface
from brownthrower import model
from brownthrower.interface import constants

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
            # TODO: Unificar els missatges de la consola. ERROR, WARNING i OK. Colors.
            print "ERROR: The task '%s' is not currently available in this environment." % items[0]
            return
        
        try:
            job = model.Job(
                task   = items[0],
                status = constants.JobStatus.STASHED
            )
            model.session.add(job)
            model.session.flush()
            # Prefetch job.id
            job_id = job.id
            
            model.session.commit()
            print "A new job for task '%s' with id %d has been created." % (items[0], job_id)
        except model.StatementError:
            print "ERROR: The job could not be created."
        finally:
            model.session.rollback()

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
            t = prettytable.PrettyTable(['id', 'event_id', 'task', 'status'])
            
            jobs = query.all()
            for job in jobs:
                t.add_row([job.id, job.event_id, job.task, job.status])
            
            if not jobs:
                print "ERROR: No jobs found matching the supplied criteria."
                return
            
            model.session.commit()
            
            print t
        
        except model.StatementError:
            print "ERROR: Could not complete the query to the database."
        finally:
            model.session.rollback()

class JobRemove(Command):
    
    def help(self, items):
        print textwrap.dedent("""\
        usage: job remove <id>
        
        Remove the job with the supplied id from the stash.
        """)
    
    def complete(self, text, items):
        return [text]
    
    def do(self, items):
        if len(items) != 1:
            return self.help(items)
        
        try:
            deleted = model.session.query(model.Job).filter_by(
                id     = items[0],
                status = constants.JobStatus.STASHED,
            ).delete(synchronize_session=False)
            
            model.session.commit()
            
            if deleted:
                print "The job has been successfully removed from the stash."
            else: # deleted == 0
                print "ERROR: The job could not be removed."
        except model.StatementError:
            print "ERROR: Could not complete the query to the database."
        finally:
            model.session.rollback()

class JobSubmit(Command):
    
    def __init__(self, tasks, *args, **kwargs):
        super(JobSubmit, self).__init__(*args, **kwargs)
        self._tasks   = tasks
    
    def help(self, items):
        print textwrap.dedent("""\
        usage: job submit <id>
        
        Mark the specified job as ready to be executed whenever there are resources available.
        """)
    
    def complete(self, text, items):
        return [text]
    
    def do(self, items):
        if len(items) != 1:
            return self.help(items)
        
        try:
            job = model.session.query(model.Job).filter_by(
                id     = items[0],
                status = constants.JobStatus.STASHED,
            ).with_lockmode('update').first()
            
            if not job:
                print "ERROR: Could not lock the job for submitting."
                return
            
            task = self._tasks.get(job.task)
            if not task:
                print "ERROR: The task '%s' is not currently available in this environment." % job.task
                return
            
            task.check_config(job.config)
            
            # TODO: Shall reset all the other fields
            job.status = constants.JobStatus.READY
            model.session.commit()
            
            print "The job has been successfully marked as ready for execution."
        
        except interface.TaskValidationException:
            print "ERROR: The job has an invalid config."
        except model.StatementError:
            print "ERROR: Could not complete the query to the database."
        finally:
            model.session.rollback()

class JobReset(Command):
    
    def help(self, items):
        print textwrap.dedent("""\
        usage: job reset <id>
        
        Return the specified job to the stash.
        """)
    
    def complete(self, text, items):
        return [text]
    
    def do(self, items):
        if len(items) != 1:
            return self.help(items)
        
        try:
            resetted = model.session.query(model.Job).filter(
                model.Job.id == items[0],
                model.Job.status.in_([
                    constants.JobStatus.READY,
                    constants.JobStatus.SUBMIT_FAIL,
                    constants.JobStatus.FAILED,
                ])
            ).update(
                #TODO: Shall reset all the other fields
                {'status' : constants.JobStatus.STASHED},
                synchronize_session = False \
            )
            model.session.commit()
            
            if resetted:
                print "The job has been successfully returned to the stash."
            else: # resetted == 0
                print "ERROR: The job could not be returned to the stash."
        except model.StatementError:
            print "ERROR: Could not complete the query to the database."
        finally:
            model.session.rollback()

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
            parent = model.session.query(model.Job).filter_by(
                id = items[0],
            ).with_lockmode('read').first()
            
            child = model.session.query(model.Job).filter_by(
                id     = items[1],
                status = constants.JobStatus.STASHED
            ).with_lockmode('read').first()
            
            if not (parent and child):
                print "ERROR: It is not possible to establish a parent-child dependency between these jobs."
                return
            
            dependency = model.JobDependency(
                child_job_id  = child.id,
                parent_job_id = parent.id
            )
            model.session.add(dependency)
            model.session.commit()
            
            print "The parent-child dependency has been successfully established."
            
        except model.StatementError:
            print "ERROR: Could not complete the query to the database."
        finally:
            model.session.rollback()

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
            parent = model.session.query(model.Job).filter_by(
                id = items[0],
            ).with_lockmode('read').first()
            
            child = model.session.query(model.Job).filter_by(
                id     = items[1],
                status = constants.JobStatus.STASHED,
            ).with_lockmode('read').first()
            
            if not (parent and child):
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
                print "The parent-child dependency has been successfully removed."
        
        except model.StatementError:
            print "ERROR: Could not complete the query to the database."
        finally:
            model.session.rollback()

class JobCancel(Command):
    
    def help(self, items):
        print textwrap.dedent("""\
        usage: job cancel <id>
        
        Cancel the specified job as soon as possible.
        """)
    
    def complete(self, text, items):
        return [text]
    
    def do(self, items):
        if len(items) != 1:
            return self.help(items)
        
        try:
            cancel =  model.session.query(model.Job).filter(
                model.Job.id == items[0],
                model.Job.status.in_([
                    constants.JobStatus.QUEUED,
                    constants.JobStatus.RUNNING,
                ])).update(
                    #TODO: Shall reset all the other fields
                    {'status' : constants.JobStatus.CANCEL},
                synchronize_session = False \
            )
            model.session.commit()
            
            if cancel:
                print "The job has been marked to be cancelled as soon as possible."
            else: # cancel == 0
                print "ERROR: No jobs could be cancelled matching the supplied criteria."
        
        except model.StatementError:
            print "ERROR: Could not complete the query to the database."
        finally:
            model.session.rollback()

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
                id     = items[1],
                status = constants.JobStatus.STASHED,
            ).with_lockmode('update').first()
            
            if not job:
                print "ERROR: Could not lock the job for submitting."
                return
            
            task = self._tasks.get(job.task)
            if not task:
                print "The task '%s' is not currently available in this environment."
                return
            
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
            
            check(new_value)
            
            setattr(job, field.key, new_value)
            model.session.commit()
            
            print "The job dataset has been successfully modified."
        
        except EnvironmentError:
            print "ERROR: Unable to open the temporary dataset buffer."
        except interface.TaskValidationException:
            print "ERROR: The new value for the dataset is not valid."
        except model.StatementError:
            print "ERROR: Could not complete the query to the database."
        finally:
            model.session.rollback()
