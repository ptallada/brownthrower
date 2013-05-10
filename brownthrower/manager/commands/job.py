#!/usr/bin/env python
# -*- coding: utf-8 -*-

import errno
import logging
import subprocess
import tempfile
import transaction

log = logging.getLogger('brownthrower.manager')

from .base import Command, error, warn, success, strong
from brownthrower import api, model
from brownthrower.interface import constants
from sqlalchemy.exc import IntegrityError, StatementError
from sqlalchemy.orm import joinedload
from sqlalchemy.orm.exc import NoResultFound
from tabulate import tabulate

class JobCreate(Command):
    """\
    usage: job create <task> [input <reference>] [config <reference>]
    
    Create a new job of the given task.
    Optionally, a reference can be specified to indicate the initial value of the
    input and config datasets. The valid values for these references are:
      - default : the default dataset for this task.
      - sample  : the sample dataset for this task
      - <name>  : a user defined dataset for this task
    """
    
    def __init__(self, *args, **kwargs):
        super(JobCreate, self).__init__(*args, **kwargs)
        
        self._profile = {}
        self._profile['config'] = api.profile.config
        self._profile['input']  = api.profile.input
    
    def complete(self, text, items):
        if not items:
            matching = [key
                        for key in api.get_tasks().iterkeys()
                        if key.startswith(text)]
            
            return matching
        
        elif (
            (len(items) == 1) and 
            (items[0] in api.get_tasks().iterkeys())
        ):
            available = ['config', 'input']
            return [
                name
                for name in sorted(available)
                if name.startswith(text)
            ]
        
        elif (
            (len(items) == 2) and
            (items[0] in api.get_tasks().iterkeys()) and
            (items[1] in ['config', 'input'])
        ):
            available = ['sample']
            if self._profile[items[1]].get_default(items[0]):
                available.append('default')
            available.extend(self._profile[items[1]].get_available(items[0]))
            return [
                name
                for name in sorted(available)
                if name.startswith(text)
            ]
        
        elif (
            (len(items) == 3) and
            (items[0] in api.get_tasks().iterkeys()) and
            (items[1] in ['config', 'input']) and
            (items[2] in self._profile[items[1]].get_available(items[0]))
        ):
            return 'config' if items[1] == 'input' else 'input'
        
        elif (
            (len(items) == 4) and
            (items[0] in api.get_tasks().iterkeys()) and
            (items[1] in ['config', 'input']) and
            (items[2] in self._profile[items[1]].get_available(items[0]))
            (items[3] == 'config' if items[1] == 'input' else 'input')
        ):
            available = ['sample']
            if self._profile[items[3]].get_default(items[0]):
                available.append('default')
            available.extend(self._profile[items[3]].get_available(items[0]))
            return [
                name
                for name in sorted(available)
                if name.startswith(text)
            ]
    
    def do(self, items):
        if (
            (len(items) not in [1, 3, 5]) or
            ( (len(items) == 3) and (items[1]) not in ['config', 'input'] ) or
            ( (len(items) == 5) and (set([items[1], items[3]]) == set(['config', 'input'])) )
        ):
            return self.help(items)
        
        try:
            task = api.get_task(items[0])
            
            reference = {
                'config' : self._profile['config'].get_default(items[0]) or 'sample',
                'input'  : self._profile['input' ].get_default(items[0]) or 'sample',
            }
            if len(items) == 3:
                reference[items[1]] = items[2]
            if len(items) == 5:
                reference[items[3]] = items[4]
            
            contents = {}
            for dataset in ['config', 'input']:
                name = reference[dataset]
                if name == 'sample':
                    contents[dataset] = api.task.get_dataset(dataset, name)(task)
                    continue
                
                path = self._profile[dataset].get_dataset_path(items[0], name)
                contents[dataset] = open(path, 'r').read()
            
            job = api.task.create(items[0])
            job.config = contents['config']
            job.input  = contents['input']
            
            job_id = job.id 
            transaction.commit()
            success("A new job for task '%s' with id %d has been created." % (items[0], job_id))
        
        except Exception as e:
            try:
                raise
            except api.dataset.NoProfileIsActive:
                error("No configuration profile is active at this time. Please, switch into one.")
            except api.task.UnavailableException:
                error("The task '%s' is not available in this environment." % e.task)
            except StatementError:
                error("The job could not be created.")
            except IOError:
                if e.errno != errno.ENOENT:
                    raise
            finally:
                log.debug(e)
        finally:
            transaction.abort()

class JobList(Command):
    """\
    usage: job list
    
    Show a list of all the jobs registered in the database.
    """
    
    def do(self, items):
        if len(items) != 0:
            return self.help(items)
        
        try:
            session = model.session_maker()
            
            table = []
            headers = (
                'id', 'super_id',
                'task', 'status',
                'created', 'queued', 'started', 'ended'
            )            
            jobs = session.query(model.Job).order_by(model.Job.id).all()
            for job in jobs:
                table.append([
                    job.id, job.super_id,
                    job.task, job.status,
                    job.ts_created.strftime('%Y-%m-%d %H:%M:%S') if job.ts_created else None,
                    job.ts_queued.strftime('%Y-%m-%d %H:%M:%S')  if job.ts_queued else None,
                    job.ts_started.strftime('%Y-%m-%d %H:%M:%S') if job.ts_started else None,
                    job.ts_ended.strftime('%Y-%m-%d %H:%M:%S')   if job.ts_ended else None,
                ])
            
            if not jobs:
                warn("No jobs found were found.")
                return
            
            transaction.commit()
            
            print tabulate(table, headers=headers)
        
        except Exception as e:
            try:
                raise
            except StatementError:
                error("Could not complete the query to the database.")
            finally:
                log.debug(e)
        finally:
            transaction.abort()

class JobShow(Command):
    """\
    usage: job show <id>
    
    Show detailed information about the job with the given id.
    """
    
    def do(self, items):
        if len(items) != 1:
            return self.help(items)
        
        try:
            session = model.session_maker()
            
            job = session.query(model.Job).filter_by(id = items[0]).options(
                joinedload(model.Job.parents),
                joinedload(model.Job.children),
                joinedload(model.Job.subjobs),
            ).first()
            
            if not job:
                error("Could not found the job with id %s." % items[0])
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
            
            table = []
            headers = ('kind', 'id', 'super_id', 'task', 'status', 'has config', 'has input', 'has output')
            
            for parent in job.parents:
                table.append(['PARENT', parent.id, parent.super_id, parent.task, parent.status, parent.config != None, parent.input != None, parent.output != None])
            table.append(['#####', job.id, job.super_id, job.task, job.status, job.config != None, job.input != None, job.output != None])
            for child in job.children:
                table.append(['CHILD', child.id, child.super_id, child.task, child.status, child.config != None, child.input != None, child.output != None])
            
            print strong("\nPARENT/CHILD JOBS:")
            print tabulate(table, headers=headers)
            
            table = []
            if job.superjob:
                table.append(['SUPER',  job.superjob.id, job.superjob.super_id, job.superjob.task, job.superjob.status, job.superjob.config != None, job.superjob.input != None, job.superjob.output != None])
            table.append(['#####', job.id, job.super_id, job.task, job.status, job.config != None, job.input != None, job.output != None])
            for subjob in job.subjobs:
                table.append(['SUB', subjob.id, subjob.super_id, subjob.task, subjob.status, subjob.config != None, subjob.input != None, subjob.output != None])
            
            transaction.commit()
            
            print strong("\nSUPER/SUB JOBS:")
            print tabulate(table, headers=headers)
        
        except Exception as e:
            try:
                raise
            except StatementError:
                error("Could not complete the query to the database.")
            finally:
                log.debug(e)
        finally:
            transaction.abort()

class JobRemove(Command):
    """\
    usage: job remove <id>
    
    Remove the job with the given id from the stash.
    """
    
    def do(self, items):
        if len(items) != 1:
            return self.help(items)
        
        try:
            api.task.remove(items[0])
            transaction.commit()
            success("The job has been successfully removed.")
        
        except Exception as e:
            try:
                raise
            except api.task.InvalidStatusException:
                error(e.message)
            except NoResultFound:
                error("The specified job does not exist.")
            except IntegrityError:
                error("Some dependencies prevent this job from being deleted.")
            except StatementError:
                error("Could not complete the query to the database.")
            finally:
                log.debug(e)
        finally:
            transaction.abort()

class JobSubmit(Command):
    """\
    usage: job submit <id>
    
    Mark the job with the given id as ready to be executed whenever there are resources available.
    """
    
    def do(self, items):
        if len(items) != 1:
            return self.help(items)
        
        try:
            api.task.submit(items[0])
            transaction.commit()
            success("The job has been successfully marked as ready for execution.")
        
        except Exception as e:
            try:
                raise
            except NoResultFound:
                error("The specified job does not exist.")
            except api.task.InvalidStatusException:
                error(e.message)
            except api.task.UnavailableException:
                error("The task '%s' is not available in this environment." % e.task)
            except api.task.ValidationException:
                error("The job has an invalid config or input.")
            except StatementError:
                error("Could not complete the query to the database.")
            finally:
                log.debug(e)
        finally:
            transaction.abort()

class JobReset(Command):
    """\
    usage: job reset <id>
    
    Return the job with the given id to the stash.
    """
    
    def do(self, items):
        if len(items) != 1:
            return self.help(items)
        
        try:
            api.task.reset(items[0])
            transaction.commit()
            success("The job has been successfully returned to the stash.")
        
        except Exception as e:
            try:
                raise
            except NoResultFound:
                error("The specified job does not exist.")
            except api.task.InvalidStatusException:
                error(e.message)
            except StatementError:
                error("Could not complete the query to the database.")
            finally:
                log.debug(e)
        finally:
            transaction.abort()

class JobLink(Command):
    """\
    usage: job link <parent_id> <child_id>
    
    Establish a dependency between two jobs.
    """
    
    def do(self, items):
        if len(items) != 2:
            return self.help(items)
        
        try:
            api.task.link(items[0], items[1])
            transaction.commit()
            success("The parent-child dependency has been successfully established.")
            
        except Exception as e:
            try:
                raise
            except api.task.InvalidStatusException:
                error(e.message)
            except NoResultFound:
                error("One of the specified jobs does not exist.")
            except StatementError:
                error("Could not complete the query to the database.")
            finally:
                log.debug(e)
        finally:
            transaction.abort()

class JobUnlink(Command):
    """\
    usage: job unlink <parent_id> <child_id>
    
    Remove the dependency between the specified jobs.
    """
    
    def do(self, items):
        if len(items) != 2:
            return self.help(items)
        
        try:
            session = model.session_maker()
            
            parent = session.query(model.Job).filter_by(
                id = items[0],
            ).with_lockmode('read').one()
            
            child = session.query(model.Job).filter_by(
                id     = items[1],
            ).with_lockmode('read').one()
            
            if child.status != constants.JobStatus.STASHED:
                error("The child job must be in the stash.")
                return
            
            if parent.super_id or child.super_id:
                error("A parent-child dependency can only be manually removed between top-level jobs.")
                return
            
            deleted = session.query(model.Dependency).filter_by(
                parent_job_id = parent.id,
                child_job_id  = child.id
            ).delete(synchronize_session=False)
            transaction.commit()
            
            if not deleted:
                error("Could not remove the parent-child dependency.")
            else:
                success("The parent-child dependency has been successfully removed.")
        
        except Exception as e:
            try:
                raise
            except NoResultFound:
                error("One of the specified jobs does not exist.")
            except StatementError:
                error("Could not complete the query to the database.")
            finally:
                log.debug(e)
        finally:
            transaction.abort()

class JobCancel(Command):
    """\
    usage: job cancel <id>
    
    Cancel the job with the given id as soon as possible.
    """
    
    def do(self, items):
        if len(items) != 1:
            return self.help(items)
        
        try:
            api.task.cancel(items[0])
            transaction.commit()
            success("The job has been marked to be cancelled as soon as possible.")
        
        except Exception as e:
            try:
                raise
            except NoResultFound:
                error("The specified job does not exist.")
            except api.task.InvalidStatusException:
                error(e.message)
            except StatementError:
                error("Could not complete the query to the database.")
            finally:
                log.debug(e)
        finally:
            transaction.abort()

class JobEdit(Command):
    """\
    usage: job edit { 'input' | 'config' } <id>
    
    Edit the specified dataset of the job with the given id.
    """
    
    def complete(self, text, items):
        if not items:
            matching = [attr
                        for attr in ['config', 'input']
                        if attr.startswith(text)]
            return matching
    
    def do(self, items):
        if (
            (len(items) != 2) or
            (items[0] not in ['config', 'input'])
        ):
            return self.help(items)
        
        try:
            session = model.session_maker()
            
            job = session.query(model.Job).filter_by(id = items[1]).with_lockmode('update').one()
            
            if job.status != constants.JobStatus.STASHED:
                error("This job is not editable in its current status.")
                return
            
            task = api.get_task(job.task)
            
            sample   = api.task.get_dataset(items[0], 'sample')(task)
            validate = api.task.get_validator(items[0])
            
            current_value = getattr(job, items[0])
            if not current_value:
                current_value = sample
            
            with tempfile.NamedTemporaryFile("w+") as fh:
                fh.write(current_value)
                fh.flush()
                
                subprocess.check_call(['editor', fh.name])
                
                fh.seek(0)
                new_value = fh.read()
            
            validate(task, new_value)
            
            setattr(job, items[0], new_value)
            transaction.commit()
            
            success("The job dataset has been successfully modified.")
            
        except Exception as e:
            try:
                raise
            except api.task.UnavailableException:
                error("The task '%s' is not available in this environment." % e.task)
            except NoResultFound:
                error("The specified job does not exist.")
            except EnvironmentError:
                error("Unable to open the temporary dataset buffer.")
            except api.task.ValidationException:
                error("The new value for the %s is not valid." % items[0])
            except StatementError:
                error("Could not complete the query to the database.")
            finally:
                log.debug(e)
        finally:
            transaction.abort()

class JobOutput(Command):
    """\
    usage: job output <id>
    
    Show the output of the finished job with the given id.
    """
    
    def do(self, items):
        if len(items) != 1:
            return self.help(items)
        
        try:
            session = model.session_maker()
            
            job = session.query(model.Job).filter_by(id = items[0]).one()
            
            if job.status != constants.JobStatus.DONE:
                error("This job is not finished yet.")
                return
            
            job_output = job.output
            
            transaction.commit()
            
            viewer = subprocess.Popen(['pager'], stdin=subprocess.PIPE)
            viewer.communicate(input=job_output)
        
        except Exception as e:
            try:
                raise
            except NoResultFound:
                error("The specified job does not exist.")
            except StatementError:
                error("Could not complete the query to the database.")
            finally:
                log.debug(e)
        finally:
            transaction.abort()
