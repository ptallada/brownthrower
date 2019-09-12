#!/usr/bin/env python
# -*- coding: utf-8 -*-

import brownthrower as bt
import errno
import logging
import readline # @UnresolvedImport
import os
import pyparsing as pp
import subprocess
import tempfile
import textwrap
import yaml

from .base import Command, error, warn, success, strong

from sqlalchemy.exc import IntegrityError, DataError, DBAPIError
from sqlalchemy.orm import joinedload, undefer_group, undefer
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql.expression import literal
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
                        for key in bt.tasks.keys()
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
            with bt.transactional_session(self.session_maker) as session:
                session.add(job)
                session.flush()
                return job.id
        
        try:
            job = bt.Job(name = items[0])
            
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
                
                
#             job.config = contents['config']
#             job.input  = contents['input']
            job_id = _add(job)
            
            success("A new job for task '%s' with id %d has been created." % (items[0], job_id))
        
        except IOError as e:
            #try:
            #    raise
            #except api.dataset.NoProfileIsActive:
            #    error("No configuration profile is active at this time. Please, switch into one.")
            #except api.task.UnavailableException:
            #    error("The task '%s' is not available in this environment." % e.task)
            #except IOError:
            if e.errno != errno.ENOENT:
                raise
            log.debug(e)

class JobList(Command):
    """\
    usage: job list [ filter ... ]
     
    Show a list of all the jobs registered in the database.
    
    The results can be filtered applying restrictions to the value of the
    different columns. The syntax of a filter is:
    
    <field><operator><value>
    
    Note that there is no space between each component.
    Allowed values for field are [id, super_id, name, status]
    Supported operators are [<, <=, =, !=, >=, >]
    
    Examples:
        id>1234
        status!=DONE
        name=myjob
    """
    
    class JobFilter(object):
        field = (
            pp.Literal('id')       |
            pp.Literal('super_id') |
            pp.Literal('name')     |
            pp.Literal('status')
        ).setResultsName('field')
        
        operator = (
            pp.Literal('<' ) |
            pp.Literal('<=') |
            pp.Literal('=' ) |
            pp.Literal('!=') |
            pp.Literal('>=') |
            pp.Literal('>' )
        ).setResultsName('operator')
        
        value = pp.Word(pp.alphanums + '_.').setResultsName('value')
        
        grammar = field + operator + value
        
        @classmethod
        def parse(cls, items):
            criteria = literal(True)
            for item in items:
                filter_ = cls.grammar.parseString(item)
                criteria &= getattr(bt.Job, filter_.field).op(filter_.operator)(filter_.value)
            
            return criteria
    
    def do(self, items):
        try:
            crit = self.JobFilter.parse(items)
            
            with bt.transactional_session(self.session_maker) as session:
                jobs = session.query(bt.Job).filter(crit).order_by(bt.Job.id).all()
                
                if not jobs:
                    warn("No jobs found were found.")
                    return
                
                table = []
                headers = (
                    'id', 'super_id',
                    'name', 'status',
                    'created', 'queued', 'started', 'ended'
                )
                for job in jobs:
                    table.append([
                        job.id, job.super_id,
                        job.name, job.status,
                        job.ts_created.strftime('%Y-%m-%d %H:%M:%S') if job.ts_created else None,
                        job.ts_queued.strftime('%Y-%m-%d %H:%M:%S')  if job.ts_queued else None,
                        job.ts_started.strftime('%Y-%m-%d %H:%M:%S') if job.ts_started else None,
                        job.ts_ended.strftime('%Y-%m-%d %H:%M:%S')   if job.ts_ended else None,
                    ])
            
            print(tabulate(table, headers=headers))
        
        except pp.ParseException as e:
            error("One of the filters has the wrong syntax.")
            log.debug(e)
        except DataError as e:
            error("One of the values whas the wrong type for the field.")
            log.debug(e)

class JobTags(Command):
    """\
    usage: job tags <id>
     
    Show all information about the tags of the job with the given id.
    """
     
    def do(self, items):
        if len(items) != 1:
            return self.help(items)
         
        try:
            with bt.transactional_session(self.session_maker) as session:
                job = session.query(bt.Job).filter_by(id = items[0]).one()
                
                for name, value in job.tag.items():
                    print(strong("### %s:" % name))
                    print(value)
                    print()
        
        except (DataError, NoResultFound) as e:
            error("The specified job does not exist.")
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
            with bt.transactional_session(self.session_maker) as session:
                job = session.query(bt.Job).filter_by(
                    id = items[0]
                ).options(undefer_group('yaml'), undefer('description')).one()
                
                print(strong("### JOB DETAILS:"))
                for field in ['id', 'super_id', 'name', 'status', 'token', 'ts_created', 'ts_queued', 'ts_started', 'ts_ended']:
                    print(field.ljust(10) + ' : ' + str(getattr(job, field)))
                print()
                print(strong("### JOB DESCRIPTION:"))
                print(job.description if job.description else '')
                print()
                print(strong("### JOB CONFIG:"))
                print(job.raw_config.strip() if job.raw_config else '...')
                print()
                print(strong("### JOB INPUT:"))
                print(job.raw_input.strip()  if job.raw_input  else '...')
                print()
                print(strong("### JOB OUTPUT:"))
                print(job.raw_output.strip() if job.raw_output else '...')
        
        except (DataError, NoResultFound) as e:
            error("The specified job does not exist.")
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
            with bt.transactional_session(self.session_maker) as session:
                job = session.query(bt.Job).filter_by(id = items[0]).options(
                    joinedload(bt.Job.parents),
                    joinedload(bt.Job.children),
                    joinedload(bt.Job.subjobs),
                ).one()
                
                table = []
                headers = ('kind', 'id', 'super_id', 'name', 'status', 'created', 'queued', 'started', 'ended')
                 
                for parent in job.parents:
                    table.append([
                        'PARENT', parent.id, parent.super_id, parent.name, parent.status,
                        parent.ts_created.strftime('%Y-%m-%d %H:%M:%S') if parent.ts_created else None,
                        parent.ts_queued.strftime('%Y-%m-%d %H:%M:%S')  if parent.ts_queued  else None,
                        parent.ts_started.strftime('%Y-%m-%d %H:%M:%S') if parent.ts_started else None,
                        parent.ts_ended.strftime('%Y-%m-%d %H:%M:%S')   if parent.ts_ended   else None,
                    ])
                table.append([
                    '#####', job.id, job.super_id, job.name, job.status,
                    job.ts_created.strftime('%Y-%m-%d %H:%M:%S') if job.ts_created else None,
                    job.ts_queued.strftime('%Y-%m-%d %H:%M:%S')  if job.ts_queued  else None,
                    job.ts_started.strftime('%Y-%m-%d %H:%M:%S') if job.ts_started else None,
                    job.ts_ended.strftime('%Y-%m-%d %H:%M:%S')   if job.ts_ended   else None,
                ])
                for child in job.children:
                    table.append([
                        'CHILD', child.id, child.super_id, child.name, child.status,
                        child.ts_created.strftime('%Y-%m-%d %H:%M:%S') if child.ts_created else None,
                        child.ts_queued.strftime('%Y-%m-%d %H:%M:%S')  if child.ts_queued  else None,
                        child.ts_started.strftime('%Y-%m-%d %H:%M:%S') if child.ts_started else None,
                        child.ts_ended.strftime('%Y-%m-%d %H:%M:%S')   if child.ts_ended   else None,
                    ])
                
                print(strong("PARENT/CHILD JOBS:"))
                print(tabulate(table, headers=headers))
                 
                table = []
                if job.superjob:
                    table.append([
                        'SUPER',  job.superjob.id, job.superjob.super_id, job.superjob.name, job.superjob.status,
                        job.superjob.ts_created.strftime('%Y-%m-%d %H:%M:%S') if job.superjob.ts_created else None,
                        job.superjob.ts_queued.strftime('%Y-%m-%d %H:%M:%S')  if job.superjob.ts_queued  else None,
                        job.superjob.ts_started.strftime('%Y-%m-%d %H:%M:%S') if job.superjob.ts_started else None,
                        job.superjob.ts_ended.strftime('%Y-%m-%d %H:%M:%S')   if job.superjob.ts_ended   else None,
                    ])
                table.append([
                    '#####', job.id, job.super_id, job.name, job.status,
                    job.ts_created.strftime('%Y-%m-%d %H:%M:%S') if job.ts_created else None,
                    job.ts_queued.strftime('%Y-%m-%d %H:%M:%S')  if job.ts_queued  else None,
                    job.ts_started.strftime('%Y-%m-%d %H:%M:%S') if job.ts_started else None,
                    job.ts_ended.strftime('%Y-%m-%d %H:%M:%S')   if job.ts_ended   else None,
                ])
                for subjob in job.subjobs:
                    table.append([
                        'SUB', subjob.id, subjob.super_id, subjob.name, subjob.status,
                        subjob.ts_created.strftime('%Y-%m-%d %H:%M:%S') if subjob.ts_created else None,
                        subjob.ts_queued.strftime('%Y-%m-%d %H:%M:%S')  if subjob.ts_queued  else None,
                        subjob.ts_started.strftime('%Y-%m-%d %H:%M:%S') if subjob.ts_started else None,
                        subjob.ts_ended.strftime('%Y-%m-%d %H:%M:%S')   if subjob.ts_ended   else None,
                    ])
                
                print()
                print(strong("SUPER/SUB JOBS:"))
                print(tabulate(table, headers=headers))
        
        except (DataError, NoResultFound) as e:
            error("The specified job does not exist.")
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
            with bt.transactional_session(self.session_maker) as session:
                job = session.query(bt.Job).filter_by(id = job_id).one()
                job.remove()
        
        try:
            _remove(items[0])
             
            success("The job has been successfully removed.")
        
        except bt.InvalidStatusException as e:
            error(e.message)
            log.debug(e)
        except (DataError, NoResultFound):
            error("The specified job does not exist.")
            log.debug(e)
        except IntegrityError:
            error("Some dependencies prevent this job from being deleted.")
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
            with bt.transactional_session(self.session_maker) as session:
                job = session.query(bt.Job).filter_by(id = job_id).one()
                job.submit()
        
        try:
            _submit(items[0])
             
            success("The job has been successfully marked as ready for execution.")
         
        except bt.InvalidStatusException as e:
            error(e.message)
            log.debug(e)
        except (DataError, NoResultFound):
            error("The specified job does not exist.")

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
            with bt.transactional_session(self.session_maker) as session:
                job = session.query(bt.Job).filter_by(id = job_id).one()
                job.reset()
        
        try:
            _reset(items[0])
             
            success("The job has been successfully returned to the stash.")
        
        except bt.InvalidStatusException as e:
            error(e.message)
            log.debug(e)
        except (DataError, NoResultFound):
            error("The specified job does not exist.")

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
            with bt.transactional_session(self.session_maker) as session:
                parent = session.query(bt.Job).filter_by(id = parent_id).one()
                child  = session.query(bt.Job).filter_by(id = child_id).one()
                parent.children.add(child)
        
        try:
            _link(items[0], items[1])
            
            success("The parent-child dependency has been successfully established.")
        
        except bt.InvalidStatusException as e:
            error(e.message)
            log.debug(e)
        except (DataError, NoResultFound):
            error("The specified job does not exist.")

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
            with bt.transactional_session(self.session_maker) as session:
                parent = session.query(bt.Job).filter_by(id = parent_id).one()
                child  = session.query(bt.Job).filter_by(id = child_id).one()
                parent.children.remove(child)
        
        try:
            _unlink(items[0], items[1])
            
            success("The parent-child dependency has been successfully removed.")
        
        except (DataError, NoResultFound) as e:
            error("One of the specified jobs does not exist.")
            log.debug(e)

class JobAbort(Command):
    """\
    usage: job abort <id>
     
    Immediately abort a running job with the given id.
    """
     
    def do(self, items):
        if len(items) != 1:
            return self.help(items)
        
        @bt.retry_on_serializable_error
        def _abort(job_id):
            with bt.transactional_session(self.session_maker) as session:
                job = session.query(bt.Job).filter_by(id = job_id).one()
                job.abort()
        
        try:
            _abort(items[0])
             
            success("The job has been aborted.")
        
        except bt.InvalidStatusException as e:
            error(e.message)
            log.debug(e)
        except (DataError, NoResultFound):
            error("The specified job does not exist.")

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
            with bt.transactional_session(self.session_maker) as session:
                job = session.query(bt.Job).filter_by(
                    id = job_id
                ).options(undefer_group('yaml')).one()
                new = job.clone()
                session.add(new)
                session.flush()
                return new.id
        
        try:
            new_id = _clone(items[0])
            
            success("Job %s has been cloned into a new job with id %d." % (items[0], new_id))
        
        except bt.InvalidStatusException as e:
            error(e.message)
            log.debug(e)
        except (DataError, NoResultFound):
            error("The specified job does not exist.")

class JobEdit(Command):
    """\
    usage: job edit { 'input' | 'config' | 'description' } <id>
      
    Edit the specified dataset of the job with the given id.
    """
      
    def complete(self, text, items):
        if not items:
            matching = [attr
                        for attr in ['config', 'input', 'description']
                        if attr.startswith(text)]
            return matching
      
    def do(self, items):
        if (
            (len(items) != 2) or
            (items[0] not in ['config', 'input', 'description'])
        ):
            return self.help(items)
        
        def _input(msg):
            readline.parse_and_bind('set disable-completion on')
            entry = input(msg)
            readline.parse_and_bind('set disable-completion off')
            readline.remove_history_item(readline.get_current_history_length()-1)
            return entry
        
        def _open_in_editor(data):
            with tempfile.NamedTemporaryFile("w+") as fh:
                fh.write(data)
                fh.flush()
                
                env = os.environ
                env['TERM'] = 'xterm'
                editor_cmd = env.get('EDITOR', 'vi')
                subprocess.check_call([editor_cmd, fh.name], env=env)
                
                fh.seek(0)
                return fh.read()
        
        def _edit_dataset(task, value):
            if task:
                doc = '# ' + '\n# '.join(textwrap.dedent(task.__doc__).splitlines()) + '\n'
            else:
                doc = textwrap.dedent("""\
                # NOTE: The documentation of the task is not available because
                #       this task is not present in this environment.
                
                """)
            original_value = doc + yaml.safe_dump(value, default_flow_style=False)
            current_value = original_value
            while True:
                new_value = _open_in_editor(current_value)
                try:
                    return yaml.safe_load(new_value)
                except yaml.YAMLError as e:
                    warn("Syntax error detected:")
                    print(e)
                    print(textwrap.dedent("""\
                    Available options:
                      u) Undo all changes and edit again
                      r) Return to editor and continue editing
                      d) Discard all changes and abort
                    """
                    ))
                    while True:
                        option = _input("Please select an option (u, r, d): ")
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
                    with bt.transactional_session(self.session_maker) as session:
                        if dataset == 'description':
                            job = session.query(bt.Job).filter_by(
                                id = job_id
                            ).options(undefer_group('desc')).one()
                            
                            current_value = job.description
                            
                            new_value = _open_in_editor(current_value)
                            job.description = new_value
                            return current_value != new_value
                        
                        else: # dataset in ['config', 'input']
                            job = session.query(bt.Job).filter_by(
                                id = job_id
                            ).options(undefer_group('yaml')).one()
                            job.assert_editable_dataset(dataset)
                            
                            current_value = job.get_dataset(dataset)
                            
                            new_value = _edit_dataset(job.task, current_value)
                            job.set_dataset(dataset, new_value)
                            return current_value != new_value
                
                except DBAPIError as e:
                    if bt.is_serializable_error(e):
                        warn("This job has received a concurrent modification.")
                        print(textwrap.dedent("""\
                        Available options:
                          r) Refresh the new values and edit again
                          d) Discard all changes and abort
                        """))
                        while True:
                            option = _input("Please select an option (r, d): ")
                            if option not in ['r', 'd']:
                                continue
                            elif option == 'd':
                                return False
                            break
                    else:
                        raise
        
        try:
            changed = _edit(items[0], items[1])
            if changed:
                success("The job has been successfully modified.")
            else:
                warn("No changes were made.")
        
        except (DataError, NoResultFound) as e:
            error("The specified job does not exist.")
            log.debug(e)
        except bt.InvalidStatusException as e:
            error(e.message)
            log.debug(e)
        except EnvironmentError as e:
            error("Unable to open the temporary dataset buffer.")
            log.debug(e)
