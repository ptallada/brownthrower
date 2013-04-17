#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import prettytable
import subprocess
import tempfile
import textwrap

log = logging.getLogger('brownthrower.manager')

from base import Command, error, warn, success, strong
from brownthrower import api
from brownthrower import interface
from brownthrower import model
from brownthrower.interface import constants

class JobCreate(Command):
    def help(self, items):
        print textwrap.dedent("""\
        usage: job create <task>
        
        Create a new job of the given task.
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
        
        try:
            job_id = api.create(items[0])
            model.session.commit() #
            success("A new job for task '%s' with id %d has been created." % (items[0], job_id))
        
        except BaseException as e:
            try:
                raise
            except KeyError:
                error("Task '%s' is not available in this environment" % items[0])
            except model.StatementError as e:
                error("The job could not be created.")
            finally:
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
            # FIXME: clean up
            table = prettytable.PrettyTable([
                'id', 'super_id',
                'task', 'status',
                'created', 'queued', 'started', 'ended',
            #    'has config', 'has input', 'has output',
            #    '# parents', '# children', '# subjobs'
            ])
            table.align = 'l'
            
            jobs = model.session.query(model.Job).options(
            #    model.joinedload(model.Job.parents),
            #    model.joinedload(model.Job.children),
            #    model.joinedload(model.Job.subjobs),
            ).limit(self._limit).all()
            for job in jobs:
                table.add_row([
                    job.id, job.super_id,
                    job.task, job.status,
                    job.ts_created.strftime('%Y-%m-%d %H:%M:%S') if job.ts_created else None,
                    job.ts_queued.strftime('%Y-%m-%d %H:%M:%S')  if job.ts_queued else None,
                    job.ts_started.strftime('%Y-%m-%d %H:%M:%S') if job.ts_started else None,
                    job.ts_ended.strftime('%Y-%m-%d %H:%M:%S')   if job.ts_ended else None,
                #    job.config != None, job.input != None, job.output != None,
                #    len(job.parents), len(job.children), len(job.subjobs)
                ])
            
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
            job = model.session.query(model.Job).filter_by(id = items[0]).options(
                model.joinedload(model.Job.parents),
                model.joinedload(model.Job.children),
                model.joinedload(model.Job.subjobs),
            ).first()
            
            if not job:
                error("Could not found the job with id %d." % items[0])
                return
            
            print strong("JOB DETAILS:")
            for field in ['id', 'super_id', 'task', 'status', 'ts_created', 'ts_queued', 'ts_started', 'ts_ended']:
                print field.ljust(10) + ' : ' + str(getattr(job, field))
            
            print strong("\nJOB CONFIG:")
            print job.config.strip() if job.config else ''
            
            print strong("\nJOB INPUT:")
            print job.input.strip()  if job.input  else ''
            
            print strong("\nJOB OUTPUT:")
            print job.output.strip() if job.output else ''
            
            table = prettytable.PrettyTable(['kind', 'id', 'super_id', 'task', 'status', 'has config', 'has input', 'has output'])
            table.align = 'l'
            
            for parent in job.parents:
                table.add_row(['PARENT', parent.id, parent.super_id, parent.task, parent.status, parent.config != None, parent.input != None, parent.output != None])
            table.add_row(['#####', job.id, job.super_id, job.task, job.status, job.config != None, job.input != None, job.output != None])
            for child in job.children:
                table.add_row(['CHILD', child.id, child.super_id, child.task, child.status, child.config != None, child.input != None, child.output != None])
            
            print strong("\nPARENT/CHILD JOBS:")
            print table
            
            table.clear_rows()
            if job.superjob:
                table.add_row(['SUPER',  job.superjob.id, job.superjob.super_id, job.superjob.task, job.superjob.status, job.superjob.config != None, job.superjob.input != None, job.superjob.output != None])
            table.add_row(['#####', job.id, job.super_id, job.task, job.status, job.config != None, job.input != None, job.output != None])
            for subjob in job.subjobs:
                table.add_row(['SUB', subjob.id, subjob.super_id, subjob.task, subjob.status, subjob.config != None, subjob.input != None, subjob.output != None])
            
            model.session.commit()
            
            print strong("\nSUPER/SUB JOBS:")
            print table
        
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
            api.remove(items[0])
            model.session.commit()
            success("The job has been successfully removed.")
        
        except BaseException as e:
            try:
                raise
            except api.InvalidStatusException as e:
                error(e.message)
            except model.NoResultFound:
                error("The specified job does not exist.")
            except model.IntegrityError:
                error("Some dependencies prevent this job from being deleted.")
            except model.StatementError:
                error("Could not complete the query to the database.")
            finally:
                log.debug(e)
        finally:
            model.session.rollback()

class JobSubmit(Command):
    
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
            api.submit(items[0])
            model.session.commit()
            success("The job has been successfully marked as ready for execution.")
        
        except BaseException as e:
            try:
                raise
            except model.NoResultFound:
                error("The specified job does not exist.")
            except api.InvalidStatusException as e:
                error(e.message)
            except interface.TaskUnavailableException as e:
                error("The task '%s' is currently not available in this environment." % e.task)
            except interface.TaskValidationException:
                error("The job has an invalid config or input.")
            except model.StatementError:
                error("Could not complete the query to the database.")
            finally:
                log.debug(e)
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
            api.reset(items[0])
            model.session.commit()
            success("The job has been successfully returned to the stash.")
        
        except BaseException as e:
            try:
                raise
            except model.NoResultFound:
                error("The specified job does not exist.")
            except api.InvalidStatusException as e:
                error(e.message)
            except model.StatementError as e:
                error("Could not complete the query to the database.")
            finally:
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
            api.link(items[0], items[1])
            model.session.commit()
            success("The parent-child dependency has been successfully established.")
            
        except BaseException as e:
            try:
                raise
            except api.InvalidStatusException as e:
                error(e.message)
            except model.NoResultFound:
                error("One of the specified jobs does not exist.")
            except model.StatementError as e:
                error("Could not complete the query to the database.")
            finally:
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
            ).with_lockmode('read').one()
            
            child = model.session.query(model.Job).filter_by(
                id     = items[1],
            ).with_lockmode('read').one()
            
            if child.status != constants.JobStatus.STASHED:
                error("The child job must be in the stash.")
                return
            
            if parent.super_id or child.super_id:
                error("A parent-child dependency can only be manually removed between top-level jobs.")
                return
            
            deleted = model.session.query(model.Dependency).filter_by(
                parent_job_id = parent.id,
                child_job_id  = child.id
            ).delete(synchronize_session=False)
            model.session.commit()
            
            if not deleted:
                error("Could not remove the parent-child dependency.")
            else:
                success("The parent-child dependency has been successfully removed.")
        
        except BaseException as e:
            try:
                raise
            except model.NoResultFound:
                error("One of the specified jobs does not exist.")
            except model.StatementError as e:
                error("Could not complete the query to the database.")
            finally:
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
            api.cancel(items[0])
            model.session.commit()
            success("The job has been marked to be cancelled as soon as possible.")
        
        except BaseException as e:
            try:
                raise
            except model.NoResultFound:
                error("The specified job does not exist.")
            except api.InvalidStatusException as e:
                error(e.message)
            except model.StatementError as e:
                error("Could not complete the query to the database.")
            finally:
                log.debug(e)
        finally:
            model.session.rollback()

class JobEdit(Command):
    
    _dataset_attr = {
        'config' : {
            'field'    : model.Job.config,
            'sample'   : lambda task: api.get_config_sample(task),
            'validate' : api.validate_config,
        },
        'input'  : {
            'field'    : model.Job.input,
            'sample'   : lambda task: api.get_input_sample(task),
            'validate' : api.validate_input,
        }
    }
    
    def __init__(self, editor, *args, **kwargs):
        super(JobEdit, self).__init__(*args, **kwargs)
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
            job = model.session.query(model.Job).filter_by(id = items[1]).with_lockmode('update').one()
            
            if job.status != constants.JobStatus.STASHED:
                error("This job is not editable in its current status.")
                return
            
            # TODO: Move this to api
            task = api.get_task(job.task)
            if not task:
                error("The task '%s' is not currently available in this environment." % job.task)
                return
            
            field    = self._dataset_attr[items[0]]['field']
            sample   = self._dataset_attr[items[0]]['sample'](task)
            validate = self._dataset_attr[items[0]]['validate']
            
            current_value = getattr(job, field.key)
            if not current_value:
                current_value = sample
            
            with tempfile.NamedTemporaryFile("w+") as fh:
                fh.write(current_value)
                fh.flush()
                
                subprocess.check_call([self._editor, fh.name])
                
                fh.seek(0)
                new_value = fh.read()
            
            validate(task, new_value)
            
            setattr(job, field.key, new_value)
            model.session.commit()
            
            success("The job dataset has been successfully modified.")
        
        except BaseException as e:
            try:
                raise
            except model.NoResultFound:
                error("The specified job does not exist.")
            except EnvironmentError:
                error("Unable to open the temporary dataset buffer.")
            except interface.TaskValidationException:
                error("The new value for the %s is not valid." % items[0])
            except model.StatementError:
                error("Could not complete the query to the database.")
            finally:
                log.debug(e)
        finally:
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
            
            job = model.session.query(model.Job).filter_by(id = items[0]).one()
            
            if job.status != constants.JobStatus.DONE:
                error("This job is not finished yet.")
                return
            
            job_output = job.output
            
            model.session.commit()
            
            viewer = subprocess.Popen([self._viewer], stdin=subprocess.PIPE)
            viewer.communicate(input=job_output)
        
        except BaseException as e:
            try:
                raise
            except model.NoResultFound as e:
                error("The specified job does not exist.")
            except model.StatementError as e:
                error("Could not complete the query to the database.")
            finally:
                log.debug(e)
        finally:
            model.session.rollback()
