#!/usr/bin/env python
# -*- coding: utf-8 -*-

import brownthrower as bt
import errno
import logging
import subprocess
import tempfile
import textwrap
import yaml

from .base import Command, error, warn, success, strong, transactional_session

from sqlalchemy.exc import IntegrityError
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
                        for key in bt.tasks.iterkeys()
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
        
        @bt.retry_on_serializable_error
        def _add(job):
            with transactional_session(self.session_maker) as session:
                session.add(job)
                session.flush()
                return job.id
        
        try:
            task = bt.tasks[items[0]]
            
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
#             job.config = contents['config']
#             job.input  = contents['input']
            job_id = _add(job)
            
            success("A new job for task '%s' with id %d has been created." % (items[0], job_id))
        
        except Exception as e:
            try:
                raise
            #except api.dataset.NoProfileIsActive:
            #    error("No configuration profile is active at this time. Please, switch into one.")
            #except api.task.UnavailableException:
            #    error("The task '%s' is not available in this environment." % e.task)
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
                jobs = session.query(bt.Job).order_by(bt.Job.id).all()
                
                if not jobs:
                    warn("No jobs found were found.")
                    return
                
                table = []
                headers = (
                    'id', 'super_id',
                    'task', 'status',
                    'created', 'queued', 'started', 'ended'
                )
                for job in jobs:
                    table.append([
                        job.id, job.super_id,
                        job.task, job.status,
                        job.ts_created.strftime('%Y-%m-%d %H:%M:%S') if job.ts_created else None,
                        job.ts_queued.strftime('%Y-%m-%d %H:%M:%S')  if job.ts_queued else None,
                        job.ts_started.strftime('%Y-%m-%d %H:%M:%S') if job.ts_started else None,
                        job.ts_ended.strftime('%Y-%m-%d %H:%M:%S')   if job.ts_ended else None,
                    ])
            
            print tabulate(table, headers=headers)
        
        except Exception as e:
            try:
                raise
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
                job = session.query(bt.Job).filter_by(id = items[0]).one()
                
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
                job = session.query(bt.Job).filter_by(id = items[0]).options(
                    joinedload(bt.Job.parents),
                    joinedload(bt.Job.children),
                    joinedload(bt.Job.subjobs),
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
        
        @bt.retry_on_serializable_error
        def _remove(job_id):
            with transactional_session(self.session_maker) as session:
                job = session.query(bt.Job).filter_by(id = job_id).one()
                job.remove()
        
        try:
            _remove(items[0])
             
            success("The job has been successfully removed.")
        
        except Exception as e:
            try:
                raise
            except bt.InvalidStatusException:
                error(e.message)
            except NoResultFound:
                error("The specified job does not exist.")
            except IntegrityError:
                error("Some dependencies prevent this job from being deleted.")
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
        
        @bt.retry_on_serializable_error
        def _submit(job_id):
            with transactional_session(self.session_maker) as session:
                job = session.query(bt.Job).filter_by(id = job_id).one()
                job.submit()
        
        try:
            _submit(items[0])
             
            success("The job has been successfully marked as ready for execution.")
         
        except Exception as e:
            try:
                raise
            except NoResultFound:
                error("The specified job does not exist.")
            except bt.InvalidStatusException:
                error(e.message)
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
        
        @bt.retry_on_serializable_error
        def _reset(job_id):
            with transactional_session(self.session_maker) as session:
                job = session.query(bt.Job).filter_by(id = job_id).one()
                job.reset()
        
        try:
            _reset(items[0])
             
            success("The job has been successfully returned to the stash.")
        
        except Exception as e:
            try:
                raise
            except NoResultFound:
                error("The specified job does not exist.")
            except bt.InvalidStatusException:
                error(e.message)
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
        
        @bt.retry_on_serializable_error
        def _link(parent_id, child_id):
            with transactional_session(self.session_maker) as session:
                parent = session.query(bt.Job).filter_by(id = parent_id).one()
                child  = session.query(bt.Job).filter_by(id = child_id).one()
                parent.children.add(child)
        
        try:
            _link(items[0], items[1])
            
            success("The parent-child dependency has been successfully established.")
        
        except Exception as e:
            try:
                raise
            except bt.InvalidStatusException:
                error(e.message)
            except NoResultFound:
                error("One of the specified jobs does not exist.")
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
        
        @bt.retry_on_serializable_error
        def _unlink(parent_id, child_id):
            with transactional_session(self.session_maker) as session:
                parent = session.query(bt.Job).filter_by(id = parent_id).one()
                child  = session.query(bt.Job).filter_by(id = child_id).one()
                parent.children.remove(child)
        
        try:
            _unlink(items[0], items[1])
            
            success("The parent-child dependency has been successfully removed.")
        
        except Exception as e:
            try:
                raise
            except NoResultFound:
                error("One of the specified jobs does not exist.")
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
        
        @bt.retry_on_serializable_error
        def _cancel(job_id):
            with transactional_session(self.session_maker) as session:
                job = session.query(bt.Job).filter_by(id = job_id).one()
                job.cancel()
        
        try:
            _cancel(items[0])
             
            success("The job has been marked to be cancelled as soon as possible.")
        
        except Exception as e:
            try:
                raise
            except NoResultFound:
                error("The specified job does not exist.")
            except bt.InvalidStatusException:
                error(e.message)
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
        
        @bt.retry_on_serializable_error
        def _clone(job_id):
            with transactional_session(self.session_maker) as session:
                job = session.query(bt.Job).filter_by(id = job_id).one()
                new = job.clone()
                session.add(new)
                session.flush()
                return new.id
        
        try:
            new_id = _clone(items[0])
            
            success("Job %s has been cloned into a new job with id %d." % (items[0], new_id))
        
        except Exception as e:
            try:
                raise
            except NoResultFound:
                error("The specified job does not exist.")
            except bt.InvalidStatusException:
                error(e.message)
            finally:
                log.debug(e)

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
        
        def _open_in_editor(data):
            with tempfile.NamedTemporaryFile("w+") as fh:
                fh.write(data)
                fh.flush()
                
                subprocess.check_call(['nano', fh.name])
                
                fh.seek(0)
                return fh.read()
        
        def _edit_dataset(value):
            original_value = yaml.safe_dump(value, default_flow_style=False)
            current_value = original_value
            while True:
                new_value = _open_in_editor(current_value)
                try:
                    return yaml.safe_load(new_value)
                except yaml.YAMLError as e:
                    warn("Syntax error detected:")
                    print e
                    print textwrap.dedent("""\
                    Available options:
                      u) Undo all changes and edit again
                      r) Return to editor and continue editing
                      d) Discard all changes and abort
                    """
                    )
                    while True:
                        option = raw_input("Please select an option (u, r, d): ")
                        if option not in ['u', 'r', 'd']:
                            continue
                        elif option == 'u':
                            current_value = original_value
                        elif option == 'r':
                            current_value = new_value
                        elif option == 'd':
                            return value
                        break
        
        def _edit(dataset, job_id):
            while True:
                try:
                    with transactional_session(self.session_maker) as session:
                        job = session.query(bt.Job).filter_by(id = job_id).one()
                        job.assert_editable_dataset(dataset)
                        
                        if job.get_raw_dataset(dataset):
                            current_value = job.get_dataset(dataset)
                        else:
                            current_value = job.get_sample(dataset)
                        
                        new_value = _edit_dataset(current_value)
                        job.set_dataset(dataset, new_value)
                        return current_value != new_value
                
                except bt.model.DBAPIError as e:
                    if bt.is_serializable_error(e):
                        warn("This job has received a concurrent modification.")
                        print textwrap.dedent("""\
                        Available options:
                          r) Refresh the new values and edit again
                          d) Discard all changes and abort
                        """)
                        while True:
                            option = raw_input("Please select an option (r, d): ")
                            if option not in ['r', 'd']:
                                continue
                            elif option == 'd':
                                return False
                            break
        
        try:
            changed = _edit(items[0], items[1])
            if changed:
                success("The job has been successfully modified.")
            else:
                warn("No changes were made.")
        
        except Exception as e:
            try:
                raise
            #except api.task.UnavailableException:
            #    error("The task '%s' is not available in this environment." % e.task)
            except NoResultFound:
                error("The specified job does not exist.")
            except bt.InvalidStatusException:
                error(e.message)
            except EnvironmentError:
                error("Unable to open the temporary dataset buffer.")
            finally:
                log.debug(e)

