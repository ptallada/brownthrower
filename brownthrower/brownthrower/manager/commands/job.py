#!/usr/bin/env python
# -*- coding: utf-8 -*-

import brownthrower
import errno
import logging
import subprocess
import tempfile
import transaction

from .base import Command, error, warn, success, strong, transactional_session

from cStringIO import StringIO
from sqlalchemy.exc import IntegrityError, StatementError
from sqlalchemy.orm import joinedload
from sqlalchemy.orm.exc import NoResultFound
from tabulate import tabulate

try:
    from logging import NullHandler
except ImportError:
    from logutils import NullHandler # @UnusedImport

log = logging.getLogger('brownthrower.manager')

class JobCreate(Command):
    """\
    usage: job create <task>
    
    Create a new job of the given task.
    """
#     """\
#     usage: job create <task> [input <reference>] [config <reference>]
#     
#     Create a new job of the given task.
#     Optionally, a reference can be specified to indicate the initial value of the
#     input and config datasets. The valid values for these references are:
#       - default : the default dataset for this task.
#       - sample  : the sample dataset for this task
#       - <name>  : a user defined dataset for this task
#     """
    
    def __init__(self, *args, **kwargs):
        super(JobCreate, self).__init__(*args, **kwargs)
        
        #self._profile = {}
        #self._profile['config'] = api.profile.config
        #self._profile['input']  = api.profile.input
    
    def complete(self, text, items):
        if not items:
            matching = [key
                        for key in brownthrower.tasks.iterkeys()
                        if key.startswith(text)]
            
            return matching
        
#         elif (
#             (len(items) == 1) and 
#             (items[0] in api.get_tasks().iterkeys())
#         ):
#             available = ['config', 'input']
#             return [
#                 name
#                 for name in sorted(available)
#                 if name.startswith(text)
#             ]
#         
#         elif (
#             (len(items) == 2) and
#             (items[0] in api.get_tasks().iterkeys()) and
#             (items[1] in ['config', 'input'])
#         ):
#             available = ['sample']
#             if self._profile[items[1]].get_default(items[0]):
#                 available.append('default')
#             available.extend(self._profile[items[1]].get_available(items[0]))
#             return [
#                 name
#                 for name in sorted(available)
#                 if name.startswith(text)
#             ]
#         
#         elif (
#             (len(items) == 3) and
#             (items[0] in api.get_tasks().iterkeys()) and
#             (items[1] in ['config', 'input']) and
#             (items[2] in self._profile[items[1]].get_available(items[0]))
#         ):
#             return 'config' if items[1] == 'input' else 'input'
#         
#         elif (
#             (len(items) == 4) and
#             (items[0] in api.get_tasks().iterkeys()) and
#             (items[1] in ['config', 'input']) and
#             (items[2] in self._profile[items[1]].get_available(items[0]))
#             (items[3] == 'config' if items[1] == 'input' else 'input')
#         ):
#             available = ['sample']
#             if self._profile[items[3]].get_default(items[0]):
#                 available.append('default')
#             available.extend(self._profile[items[3]].get_available(items[0]))
#             return [
#                 name
#                 for name in sorted(available)
#                 if name.startswith(text)
#             ]
    
    def do(self, items):
        if len(items) != 1:
            return self.help(items)
#         if (
#             (len(items) not in [1, 3, 5]) or
#             ( (len(items) == 3) and (items[1]) not in ['config', 'input'] ) or
#             ( (len(items) == 5) and (set([items[1], items[3]]) == set(['config', 'input'])) )
#         ):
#             return self.help(items)
        
        try:
            with transactional_session(self.session_maker) as session:
                task = brownthrower.tasks[items[0]]
                
#                 reference = {
#                     'config' : self._profile['config'].get_default(items[0]) or 'sample',
#                     'input'  : self._profile['input' ].get_default(items[0]) or 'sample',
#                 }
#                 if len(items) == 3:
#                     reference[items[1]] = items[2]
#                 if len(items) == 5:
#                     reference[items[3]] = items[4]
#                 
#                 contents = {}
#                 for dataset in ['config', 'input']:
#                     name = reference[dataset]
#                     if name == 'sample':
#                         contents[dataset] = api.task.get_dataset(dataset, name)(task)
#                         continue
#                     
#                     path = self._profile[dataset].get_dataset_path(items[0], name)
#                     contents[dataset] = open(path, 'r').read()
                
                job = task()
                session.add(job)
                session.flush()
#                 job.config = contents['config']
#                 job.input  = contents['input']
                job_id = job.id 
            
            success("A new job for task '%s' with id %d has been created." % (items[0], job_id))
        
        except Exception as e:
            try:
                raise
            #except api.dataset.NoProfileIsActive:
            #    error("No configuration profile is active at this time. Please, switch into one.")
            #except api.task.UnavailableException:
            #    error("The task '%s' is not available in this environment." % e.task)
            except StatementError:
                error("The job could not be created.")
            except IOError:
                if e.errno != errno.ENOENT:
                    raise
            finally:
                log.debug(e)

class JobList(Command):
    """\
    usage: job list
     
    Show a list of all the jobs registered in the database.
    """
     
    def do(self, items):
        if len(items) != 0:
            return self.help(items)
         
        try:
            with transactional_session(self.session_maker) as session:
                table = []
                headers = (
                    'id', 'super_id',
                    'task', 'status',
                    'created', 'queued', 'started', 'ended'
                )            
                jobs = session.query(brownthrower.Job).order_by(brownthrower.Job.id).all()
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
             
            print tabulate(table, headers=headers)
         
        except Exception as e:
            try:
                raise
            except StatementError:
                error("Could not complete the query to the database.")
            finally:
                log.debug(e)

class JobShow(Command):
    """\
    usage: job show <id>
     
    Show information about the attributes of the job with the given id.
    """
     
    def do(self, items):
        if len(items) != 1:
            return self.help(items)
         
        try:
            with transactional_session(self.session_maker) as session:
                job = session.query(brownthrower.Job).filter_by(id = items[0]).one()
                
                print strong("### JOB DETAILS:")
                for field in ['id', 'super_id', 'task', 'status', 'ts_created', 'ts_queued', 'ts_started', 'ts_ended']:
                    print field.ljust(10) + ' : ' + str(getattr(job, field))
                print
                print strong("### JOB CONFIG:")
                print job.config.strip() if job.config else '...'
                print
                print strong("### JOB INPUT:")
                print job.input.strip()  if job.input  else '...'
                print
                print strong("### JOB OUTPUT:")
                print job.output.strip() if job.output else '...'
         
        except Exception as e:
            try:
                raise
            except NoResultFound:
                error("The specified job does not exist.")
            except StatementError:
                error("Could not complete the query to the database.")
            finally:
                log.debug(e)

class JobGraph(Command):
    """\
    usage: job graph <id>
     
    Show dependency information about the job with the given id.
    """
     
    def do(self, items):
        if len(items) != 1:
            return self.help(items)
         
        try:
            with transactional_session(self.session_maker) as session:
                job = session.query(brownthrower.Job).filter_by(id = items[0]).options(
                    joinedload(brownthrower.Job.parents),
                    joinedload(brownthrower.Job.children),
                    joinedload(brownthrower.Job.subjobs),
                ).one()
                
                table = []
                headers = ('kind', 'id', 'super_id', 'task', 'status', 'created', 'queued', 'started', 'ended')
                 
                for parent in job.parents:
                    table.append([
                        'PARENT', parent.id, parent.super_id, parent.task, parent.status,
                        parent.ts_created.strftime('%Y-%m-%d %H:%M:%S') if parent.ts_created else None,
                        parent.ts_queued.strftime('%Y-%m-%d %H:%M:%S')  if parent.ts_queued  else None,
                        parent.ts_started.strftime('%Y-%m-%d %H:%M:%S') if parent.ts_started else None,
                        parent.ts_ended.strftime('%Y-%m-%d %H:%M:%S')   if parent.ts_ended   else None,
                    ])
                table.append([
                    '#####', job.id, job.super_id, job.task, job.status,
                    job.ts_created.strftime('%Y-%m-%d %H:%M:%S') if job.ts_created else None,
                    job.ts_queued.strftime('%Y-%m-%d %H:%M:%S')  if job.ts_queued  else None,
                    job.ts_started.strftime('%Y-%m-%d %H:%M:%S') if job.ts_started else None,
                    job.ts_ended.strftime('%Y-%m-%d %H:%M:%S')   if job.ts_ended   else None,
                ])
                for child in job.children:
                    table.append([
                        'CHILD', child.id, child.super_id, child.task, child.status,
                        child.ts_created.strftime('%Y-%m-%d %H:%M:%S') if child.ts_created else None,
                        child.ts_queued.strftime('%Y-%m-%d %H:%M:%S')  if child.ts_queued  else None,
                        child.ts_started.strftime('%Y-%m-%d %H:%M:%S') if child.ts_started else None,
                        child.ts_ended.strftime('%Y-%m-%d %H:%M:%S')   if child.ts_ended   else None,
                    ])
                
                print strong("PARENT/CHILD JOBS:")
                print tabulate(table, headers=headers)
                 
                table = []
                if job.superjob:
                    table.append([
                        'SUPER',  job.superjob.id, job.superjob.super_id, job.superjob.task, job.superjob.status,
                        job.superjob.ts_created.strftime('%Y-%m-%d %H:%M:%S') if job.superjob.ts_created else None,
                        job.superjob.ts_queued.strftime('%Y-%m-%d %H:%M:%S')  if job.superjob.ts_queued  else None,
                        job.superjob.ts_started.strftime('%Y-%m-%d %H:%M:%S') if job.superjob.ts_started else None,
                        job.superjob.ts_ended.strftime('%Y-%m-%d %H:%M:%S')   if job.superjob.ts_ended   else None,
                    ])
                table.append([
                    '#####', job.id, job.super_id, job.task, job.status,
                    job.ts_created.strftime('%Y-%m-%d %H:%M:%S') if job.ts_created else None,
                    job.ts_queued.strftime('%Y-%m-%d %H:%M:%S')  if job.ts_queued  else None,
                    job.ts_started.strftime('%Y-%m-%d %H:%M:%S') if job.ts_started else None,
                    job.ts_ended.strftime('%Y-%m-%d %H:%M:%S')   if job.ts_ended   else None,
                ])
                for subjob in job.subjobs:
                    table.append([
                        'SUB', subjob.id, subjob.super_id, subjob.task, subjob.status,
                        subjob.ts_created.strftime('%Y-%m-%d %H:%M:%S') if subjob.ts_created else None,
                        subjob.ts_queued.strftime('%Y-%m-%d %H:%M:%S')  if subjob.ts_queued  else None,
                        subjob.ts_started.strftime('%Y-%m-%d %H:%M:%S') if subjob.ts_started else None,
                        subjob.ts_ended.strftime('%Y-%m-%d %H:%M:%S')   if subjob.ts_ended   else None,
                    ])
                
                print
                print strong("SUPER/SUB JOBS:")
                print tabulate(table, headers=headers)
        
        except Exception as e:
            try:
                raise
            except NoResultFound:
                error("The specified job does not exist.")
            except StatementError:
                error("Could not complete the query to the database.")
            finally:
                log.debug(e)

class JobRemove(Command):
    """\
    usage: job remove <id>
     
    Remove the job with the given id from the stash.
    """
     
    def do(self, items):
        if len(items) != 1:
            return self.help(items)
         
        try:
            with transactional_session(self.session_maker) as session:
                job = session.query(brownthrower.Job).filter_by(id = items[0]).one()
                job.remove()
             
            success("The job has been successfully removed.")
         
        except Exception as e:
            try:
                raise
            except brownthrower.InvalidStatusException:
                error(e.message)
            except NoResultFound:
                error("The specified job does not exist.")
            except IntegrityError:
                error("Some dependencies prevent this job from being deleted.")
            except StatementError:
                error("Could not complete the query to the database.")
            finally:
                log.debug(e)

class JobSubmit(Command):
    """\
    usage: job submit <id>
     
    Mark the job with the given id as ready to be executed.
    """
     
    def do(self, items):
        if len(items) != 1:
            return self.help(items)
         
        try:
            with transactional_session(self.session_maker) as session:
                job = session.query(brownthrower.Job).filter_by(id = items[0]).one()
                job.submit()
             
            success("The job has been successfully marked as ready for execution.")
         
        except Exception as e:
            try:
                raise
            except NoResultFound:
                error("The specified job does not exist.")
            except brownthrower.InvalidStatusException:
                error(e.message)
            except StatementError:
                error("Could not complete the query to the database.")
            finally:
                log.debug(e)
 
class JobReset(Command):
    """\
    usage: job reset <id>
     
    Return the job with the given id to the stash.
    """
     
    def do(self, items):
        if len(items) != 1:
            return self.help(items)
         
        try:
            with transactional_session(self.session_maker) as session:
                job = session.query(brownthrower.Job).filter_by(id = items[0]).one()
                job.reset()
             
            success("The job has been successfully returned to the stash.")
         
        except Exception as e:
            try:
                raise
            except NoResultFound:
                error("The specified job does not exist.")
            except brownthrower.InvalidStatusException:
                error(e.message)
            except StatementError:
                error("Could not complete the query to the database.")
            finally:
                log.debug(e)

class JobLink(Command):
    """\
    usage: job link <parent_id> <child_id>
     
    Establish a dependency between two jobs.
    """
     
    def do(self, items):
        if len(items) != 2:
            return self.help(items)
         
        try:
            with transactional_session(self.session_maker) as session:
                parent = session.query(brownthrower.Job).filter_by(id = items[0]).one()
                child  = session.query(brownthrower.Job).filter_by(id = items[1]).one()
                parent.children.add(child)
             
            success("The parent-child dependency has been successfully established.")
             
        except Exception as e:
            try:
                raise
            except brownthrower.InvalidStatusException:
                error(e.message)
            except NoResultFound:
                error("One of the specified jobs does not exist.")
            except StatementError:
                error("Could not complete the query to the database.")
            finally:
                log.debug(e)

class JobUnlink(Command):
    """\
    usage: job unlink <parent_id> <child_id>
     
    Remove the dependency between the specified jobs.
    """
     
    def do(self, items):
        if len(items) != 2:
            return self.help(items)
         
        try:
            with transactional_session(self.session_maker) as session:
                parent = session.query(brownthrower.Job).filter_by(id = items[0]).one()
                child  = session.query(brownthrower.Job).filter_by(id = items[1]).one()
                parent.children.remove(child)
                
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

class JobCancel(Command):
    """\
    usage: job cancel <id>
     
    Cancel the job with the given id as soon as possible.
    """
     
    def do(self, items):
        if len(items) != 1:
            return self.help(items)
         
        try:
            with transactional_session(self.session_maker) as session:
                job = session.query(brownthrower.Job).filter_by(id = items[0]).one()
                job.cancel()
             
            success("The job has been marked to be cancelled as soon as possible.")
         
        except Exception as e:
            try:
                raise
            except NoResultFound:
                error("The specified job does not exist.")
            except brownthrower.InvalidStatusException:
                error(e.message)
            except StatementError:
                error("Could not complete the query to the database.")
            finally:
                log.debug(e)

class JobClone(Command):
    """\
    usage: job clone <id>
     
    Clone the job with the given id.
    """
     
    def do(self, items):
        if len(items) != 1:
            return self.help(items)
         
        try:
            with transactional_session(self.session_maker) as session:
                job = session.query(brownthrower.Job).filter_by(id = items[0]).one()
                new = job.clone()
                session.add(new)
                session.flush()
                new_id = new.id 
            
            success("Job %s has been cloned into a new job with id %d." % (items[0], new_id))
        
        except Exception as e:
            try:
                raise
            except NoResultFound:
                error("The specified job does not exist.")
            except brownthrower.InvalidStatusException:
                error(e.message)
            except StatementError:
                error("Could not complete the query to the database.")
            finally:
                log.debug(e)

# 
# class JobEdit(Command):
#     """\
#     usage: job edit { 'input' | 'config' } <id>
#     
#     Edit the specified dataset of the job with the given id.
#     """
#     
#     def complete(self, text, items):
#         if not items:
#             matching = [attr
#                         for attr in ['config', 'input']
#                         if attr.startswith(text)]
#             return matching
#     
#     def do(self, items):
#         if (
#             (len(items) != 2) or
#             (items[0] not in ['config', 'input'])
#         ):
#             return self.help(items)
#         
#         try:
#             session = model.session_maker()
#             with transaction.manager:
#                 job = session.query(model.Job).filter_by(id = items[1]).with_lockmode('update').one()
#                 
#                 if job.status != constants.JobStatus.STASHED:
#                     error("This job is not editable in its current status.")
#                     return
#                 
#                 task = api.get_task(job.task)
#                 
#                 sample   = api.task.get_dataset(items[0], 'sample')(task)
#                 validate = api.task.get_validator(items[0])
#                 
#                 current_value = getattr(job, items[0])
#                 if not current_value:
#                     current_value = sample
#                 
#                 with tempfile.NamedTemporaryFile("w+") as fh:
#                     fh.write(current_value)
#                     fh.flush()
#                     
#                     subprocess.check_call([settings['editor'], fh.name])
#                     
#                     fh.seek(0)
#                     new_value = fh.read()
#                 
#                 validate(task, new_value)
#                 
#                 setattr(job, items[0], new_value)
#             
#             success("The job dataset has been successfully modified.")
#             
#         except Exception as e:
#             try:
#                 raise
#             except api.task.UnavailableException:
#                 error("The task '%s' is not available in this environment." % e.task)
#             except NoResultFound:
#                 error("The specified job does not exist.")
#             except EnvironmentError:
#                 error("Unable to open the temporary dataset buffer.")
#             except api.task.ValidationException:
#                 error("The new value for the %s is not valid." % items[0])
#             except StatementError:
#                 error("Could not complete the query to the database.")
#             finally:
#                 log.debug(e)

