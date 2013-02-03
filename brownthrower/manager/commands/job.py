#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import prettytable
import subprocess
import tempfile
import textwrap

log = logging.getLogger('brownthrower.manager')

from base import Command, error, warn, success
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
            error("The task '%s' is not currently available in this environment." % items[0])
            return
        
        try:
            job = model.Job(
                task   = items[0],
                config = task.get_config_sample(),
                status = constants.JobStatus.STASHED
            )
            model.session.add(job)
            model.session.flush()
            # Prefetch job.id
            job_id = job.id
            
            model.session.commit()
            success("A new job for task '%s' with id %d has been created." % (items[0], job_id))
        
        except model.StatementError as e:
            error("The job could not be created.")
            log.debug(e)
        finally:
            model.session.rollback()

class JobList(Command):
    
    def __init__(self, limit, *args, **kwargs):
        super(JobList, self).__init__(*args, **kwargs)
        self._limit = limit
    
    def help(self, items):
        print textwrap.dedent("""\
        usage: job list
        
        Show a list of all the jobs registered in the database.
        """)
    
    def complete(self, text, items):
        return [text]
    
    def do(self, items):
        if len(items) != 0:
            return self.help(items)
        
        try:
            table = prettytable.PrettyTable(['id', 'cluster_id', 'task', 'status', 'has config', 'has input', 'has output', '# parents', '# children'])
            table.align = 'l'
            
            jobs = model.session.query(model.Job).options(model.eagerload_all(model.Job.parents, model.Job.children)).limit(self._limit).all()
            for job in jobs:
                table.add_row([job.id, job.cluster_id, job.task, job.status, job.config != None, job.input != None, job.output != None, len(job.parents), len(job.children)])
            
            if not jobs:
                warn("No jobs found were found.")
                return
            
            model.session.commit()
            
            print table
        
        except model.StatementError as e:
            error("Could not complete the query to the database.")
            log.debug(e)
        finally:
            model.session.rollback()

class JobShow(Command):
    
    def help(self, items):
        print textwrap.dedent("""\
        usage: job show <id>
        
        Show detailed information about the specified job.
        """)
    
    def complete(self, text, items):
        return [text]
    
    def do(self, items):
        if len(items) != 1:
            return self.help(items)
        
        try:
            job = model.session.query(model.Job).filter_by(id = items[0]).options(model.eagerload_all(model.Job.parents, model.Job.children)).first()
            
            if not job:
                warn("Could not found the job with id %d." % items[0])
                return
            
            table = prettytable.PrettyTable(['kind', 'id', 'cluster_id', 'task', 'status', 'has config', 'has input', 'has output'])
            table.align = 'l'
            
            for parent in job.parents:
                table.add_row(['PARENT', parent.id, parent.cluster_id, parent.task, parent.status, parent.config != None, parent.input != None, parent.output != None])
            table.add_row(['#####', job.id, job.cluster_id, job.task, job.status, job.config != None, job.input != None, job.output != None])
            for child in job.children:
                table.add_row(['CHILD', child.id, child.cluster_id, child.task, child.status, child.config != None, child.input != None, child.output != None])
            
            print table
            
            model.session.commit()
        
        except model.StatementError as e:
            error("Could not complete the query to the database.")
            log.debug(e)
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
                success("The job has been successfully removed from the stash.")
            else: # deleted == 0
                error("The job could not be removed.")
        
        except BaseException as e:
            try:
                raise
            except model.IntegrityError:
                error("Some dependencies prevent this job from being deleted.")
            except model.StatementError:
                error("Could not complete the query to the database.")
            finally:
                model.session.rollback()
                log.debug(e)

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
                error("The job could not be submitted.")
                return
            
            task = self._tasks.get(job.task)
            if not task:
                error("The task '%s' is not currently available in this environment." % job.task)
                return
            
            task.validate_config(job.config)
            
            # TODO: Shall reset all the other fields
            job.status = constants.JobStatus.READY
            model.session.commit()
            
            success("The job has been successfully marked as ready for execution.")
        
        except BaseException as e:
            try:
                raise
            except interface.TaskValidationException:
                error("The job has an invalid config.")
            except model.StatementError:
                error("Could not complete the query to the database.")
            finally:
                model.session.rollback()
                log.debug(e)

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
                success("The job has been successfully returned to the stash.")
            else: # resetted == 0
                error("The job could not be returned to the stash.")
        
        except model.StatementError as e:
            error("Could not complete the query to the database.")
            log.debug(e)
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
                warn("It is not possible to establish a parent-child dependency between these jobs.")
                return
            
            dependency = model.JobDependency(
                child_job_id  = child.id,
                parent_job_id = parent.id
            )
            model.session.add(dependency)
            model.session.commit()
            
            success("The parent-child dependency has been successfully established.")
            
        except model.StatementError as e:
            error("Could not complete the query to the database.")
            log.debug(e)
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
                warn("It is not possible to remove the parent-child dependency.")
                return
            
            deleted = model.session.query(model.JobDependency).filter_by(
                parent_job_id = parent.id,
                child_job_id  = child.id
            ).delete(synchronize_session=False)
            model.session.commit()
            
            if not deleted:
                error("Could not remove the parent-child dependency.")
            else:
                success("The parent-child dependency has been successfully removed.")
        
        except model.StatementError as e:
            error("Could not complete the query to the database.")
            log.debug(e)
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
                success("The job has been marked to be cancelled as soon as possible.")
            else: # cancel == 0
                error("The job could not be marked to be cancelled.")
        
        except model.StatementError as e:
            error("Could not complete the query to the database.")
            log.debug(e)
        finally:
            model.session.rollback()

class JobEdit(Command):
    
    _dataset_attr = {
        'config' : {
            'field'    : model.Job.config,
            'sample'   : lambda task: task.get_config_sample,
            'validate' : lambda task: task.validate_config,
        },
        'input'  : {
            'field'    : model.Job.input,
            'sample'   : lambda task: task.get_input_sample,
            'validate' : lambda task: task.validate_input,
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
                warn("Could not find or lock the job for editing.")
                return
            
            task = self._tasks.get(job.task)
            if not task:
                error("The task '%s' is not currently available in this environment." % job.task)
                return
            
            field    = self._dataset_attr[items[0]]['field']
            sample   = self._dataset_attr[items[0]]['sample'](task)()
            validate = self._dataset_attr[items[0]]['validate'](task)
            
            current_value = getattr(job, field.key)
            if not current_value:
                current_value = sample
            
            with tempfile.NamedTemporaryFile("w+") as fh:
                fh.write(current_value)
                fh.flush()
                
                subprocess.check_call([self._editor, fh.name])
                
                fh.seek(0)
                new_value = fh.read()
            
            validate(new_value)
            
            setattr(job, field.key, new_value)
            model.session.commit()
            
            success("The job dataset has been successfully modified.")
        
        except BaseException as e:
            try:
                raise
            except EnvironmentError:
                error("Unable to open the temporary dataset buffer.")
            except interface.TaskValidationException:
                error("The new value for the %s is not valid." % items[0])
            except model.StatementError:
                error("Could not complete the query to the database.")
            finally:
                log.debug(e)
                model.session.rollback()

class JobOutput(Command):
    
    def __init__(self, viewer, *args, **kwargs):
        super(JobOutput, self).__init__(*args, **kwargs)
        self._viewer = viewer
    
    def help(self, items):
        print textwrap.dedent("""\
        usage: job output <id>
        
        Show the output of a completed job.
        """)
    
    def complete(self, text, items):
        return [text]
    
    def do(self, items):
        if len(items) != 1:
            return self.help(items)
        
        try:
            job = model.session.query(model.Job).filter_by(
                id     = items[0],
                status = constants.JobStatus.DONE,
            ).first()
            
            if not job:
                warn("The output from job %d cannot be shown." % items[0])
                return
            
            job_output = job.output
            
            model.session.commit()
            
            viewer = subprocess.Popen([self._viewer], stdin=subprocess.PIPE)
            viewer.communicate(input=job_output)
        
        except model.StatementError as e:
            error("Could not complete the query to the database.")
            log.debug(e)
        finally:
            model.session.rollback()
