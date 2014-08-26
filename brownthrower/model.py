#!/usr/bin/env python
# -*- coding: utf-8 -*-

import contextlib
import logging
import warnings
import yaml

from sqlalchemy import event, func, literal_column
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship, reconstructor, deferred
from sqlalchemy.orm.collections import attribute_mapped_collection
from sqlalchemy.orm.exc import DetachedInstanceError
from sqlalchemy.orm.session import object_session
from sqlalchemy.schema import (Column, ForeignKeyConstraint, Index,
                               PrimaryKeyConstraint, UniqueConstraint)
from sqlalchemy.sql import functions
from sqlalchemy.types import DateTime, Integer, String, Text

from . import taskstore

log = logging.getLogger('brownthrower.model')

Base = declarative_base()

tasks = taskstore.TaskStore()
"""Global task container, implemented as a read-only dict."""

TAG_TRACEBACK = 'bt_traceback'
TAG_TOKEN     = 'bt_token'

def _deprecated(func):
    """
    This is a decorator which can be used to mark functions
    as deprecated. It will result in a warning being emmitted
    when the function is used.
    """
    def newFunc(*args, **kwargs):
        warnings.warn(
            "Call to deprecated function %s." % func.__name__,
            category=DeprecationWarning, stacklevel=2
        )
        return func(*args, **kwargs)
    
    newFunc.__name__ = func.__name__
    newFunc.__doc__ = func.__doc__
    newFunc.__dict__.update(func.__dict__)
    
    return newFunc

class Dependency(Base):
    """\
    Main class Dependency documentation text
    """
    
    __tablename__ = 'dependency'
    __table_args__ = (
        # Primary key
        PrimaryKeyConstraint('parent_job_id', 'child_job_id', name='pk_dependency'),
        # Foreign keys
        # FIXME: This constraints are not useful for top-level jobs
        ForeignKeyConstraint(            ['parent_job_id'],                 ['job.id'], onupdate='CASCADE', ondelete='CASCADE', name= 'fk_dependency_parent'),
        ForeignKeyConstraint(            ['child_job_id'],                  ['job.id'], onupdate='CASCADE', ondelete='CASCADE', name= 'fk_dependency_child'),
        ForeignKeyConstraint(['super_id', 'parent_job_id'], ['job.super_id', 'job.id'], onupdate='CASCADE', ondelete='CASCADE', name= 'fk_dependency_super_parent'),
        ForeignKeyConstraint(['super_id', 'child_job_id'],  ['job.super_id', 'job.id'], onupdate='CASCADE', ondelete='CASCADE', name= 'fk_dependency_super_child'),
    )
    
    # Columns
    # TODO: Rename to super_job_id, or remove 'job' from others
    _super_id  = Column('super_id',      Integer, nullable=True)
    _parent_id = Column('parent_job_id', Integer, nullable=False)
    _child_id  = Column('child_job_id',  Integer, nullable=False)
    
    @hybrid_property
    def super_id(self):
        return self._super_id
    
    @hybrid_property
    def parent_id(self):
        return self._parent_id
    
    @hybrid_property
    def child_id(self):
        return self._child_id
    
    def __repr__(self):
        return u"%s(super_id=%s, parent_job_id=%s, child_job_id=%s)" % (
            self.__class__.__name__,
            repr(self._super_id),
            repr(self._parent_id),
            repr(self._child_id),
        )

class InvalidStatusException(Exception):
    """\
    Exception that is raised when an invalid status
    """
    
    def __init__(self, message=None):
        self.message = message
        
    def __str__(self):
        return str(self.message)

class TaskNotAvailableException(Exception):
    """\
    Exception raised when a task is not available
    """
    
    def __init__(self, name):
        self.message = "Task '%s' is not available in this environment." % name
        
    def __str__(self):
        return str(self.message)

class TokenMismatchException(Exception):
    """\
    Raised when absent or different token
    """
    
    def __init__(self, message=None):
        self.message = message
        
    def __str__(self):
        return str(self.message)

class Job(Base):
    """
    Base class for user-defined Jobs.
    
    All Job subclasses must inherit from this class and override the methods
    defined in brownthrower.interface.Job.
    """
    
    ###########################################################################
    # KEYS AND INDEXES                                                        #
    ###########################################################################
    
    __tablename__ = 'job'
    __table_args__ = (
        # Primary key
        PrimaryKeyConstraint('id', name='pk_job'),
        # Unique key
        UniqueConstraint('super_id', 'id', name='uq_job_super'),
        # Foreign keys
        ForeignKeyConstraint(['super_id'], ['job.id'], onupdate='CASCADE', ondelete='RESTRICT', name='fk_job_super'),
        # Indexes
        Index('ix_job_status', 'status'),
        Index('ix_job_task',   'task'),
    )
    
    ###########################################################################
    # COLUMNS                                                                 #
    ###########################################################################
    
    _id         =          Column('id',         Integer,    nullable=False)
    _super_id   =          Column('super_id',   Integer,    nullable=True)
    # TODO: rename field to 'name' and change index too
    _name       =          Column('task',       String(50), nullable=False)
    _status     =          Column('status',     String(20), nullable=False)
    _config     = deferred(Column('config',     Text,       nullable=True), group='yaml')
    _input      = deferred(Column('input',      Text,       nullable=True), group='yaml')
    _output     = deferred(Column('output',     Text,       nullable=True), group='yaml')
    _ts_created =          Column('ts_created', DateTime,   nullable=False, default=functions.now())
    _ts_queued  =          Column('ts_queued',  DateTime,   nullable=True)
    _ts_started =          Column('ts_started', DateTime,   nullable=True)
    _ts_ended   =          Column('ts_ended',   DateTime,   nullable=True)
    
    ###########################################################################
    # RELATIONSHIPS                                                           #
    ###########################################################################
    
    parents  = relationship('Job',
        back_populates    = 'children', secondary = 'dependency',
        primaryjoin       = 'Dependency._child_id == Job._id',
        secondaryjoin     = 'Job._id == Dependency._parent_id',
        collection_class  = set)
    """parents relationship"""
    
    children = relationship('Job',
        back_populates    = 'parents',  secondary = 'dependency',
        primaryjoin       = 'Dependency._parent_id == Job._id',
        secondaryjoin     = 'Job._id == Dependency._child_id',
        collection_class  = set)
    """children relationship"""
    
    superjob = relationship('Job',
        back_populates    = 'subjobs',
        primaryjoin       = 'Job._super_id == Job._id',
        remote_side       = 'Job._id')
    """superjob relationship"""
    
    subjobs  = relationship('Job',
        back_populates    = 'superjob',
        primaryjoin       = 'Job._super_id == Job._id',
        cascade           = 'all, delete-orphan', passive_deletes = True,
        collection_class  = set)
    """subjobs relationship"""
    
    _tags    = relationship('Tag',
        cascade           = 'all, delete-orphan', passive_deletes = True,
        collection_class  = attribute_mapped_collection('_name'))
    
    ###########################################################################
    # PROXIES                                                                 #
    ###########################################################################
    
    tag = association_proxy('_tags', '_value', creator=lambda name, value: Tag(_name=name, _value=value))
    """tag association proxy"""
    
    ###########################################################################
    # STATUS                                                                  #
    ###########################################################################
    
    class Status(object):
        """\
        status class
        """
        STASHED = 'STASHED'
        """Preparation phase. It is being configured and cannot be executed yet."""
        QUEUED = 'QUEUED'
        """\
        The job has been configured and its dependencies are already set.
        It will be executed as soon as possible.
        """
        PROCESSING = 'PROCESSING'
        """The job is being processed."""
        DONE = 'DONE'
        """The job has finished successfully."""
        FAILED = 'FAILED'
        """The job did not finish successfully."""
    
    ###########################################################################
    # CONSTRUCTORS AND SPECIAL METHODS                                        #
    ###########################################################################
    
    def __init__(self, name, task = None):
        values = {
            '_name' : name,
            '_status' : Job.Status.STASHED,
            '_ts_created' : func.now(),
        }
          
        super(Job, self).__init__(**values)
        self._reconstruct()
        
        if task:
            if task._bt_name != name:
                raise ValueError("Mismatch between task name and implementer class.")
            if self._task and self._task != task:
                raise ValueError("Mismatch between internal implementer task and the provided one.")
    
    @reconstructor
    def _reconstruct(self):
        self._task = tasks.get(self.name, None)
    
    def __repr__(self):
        return u"%s(id=%s, super_id=%s, name=%s, status=%s)" % (
            self.__class__.__name__,
            repr(self._id),
            repr(self._super_id),
            repr(self._name),
            repr(self._status),
        )
    
    ###########################################################################
    # MAPPED ATTRIBUTES                                                       #
    ###########################################################################
    
    @hybrid_property
    def id(self):
        return self._id
    
    @hybrid_property
    def super_id(self):
        return self._super_id
    
    @hybrid_property
    def name(self):
        return self._name
    
    @hybrid_property
    def status(self):
        return self._status
    
    @hybrid_property
    def raw_config(self):
        return self._config
    
    @hybrid_property
    def raw_input(self):
        return self._input
    
    @hybrid_property
    def raw_output(self):
        return self._output
    
    @hybrid_property
    def ts_created(self):
        return self._ts_created
    
    @hybrid_property
    def ts_queued(self):
        return self._ts_queued
    
    @hybrid_property
    def ts_started(self):
        return self._ts_started
    
    @hybrid_property
    def ts_ended(self):
        return self._ts_ended
    
    ###########################################################################
    # DESCRIPTORS                                                             #
    ###########################################################################
    
    @property
    def task(self):
        return self._task
    
    ###########################################################################
    # COLLECTION EVENTS                                                       #
    ###########################################################################
    
    @classmethod
    def __declare_last__(cls):
        @event.listens_for(cls.children, 'append', propagate=True)
        @event.listens_for(cls.children, 'remove', propagate=True)
        def _set_parent_children(parent, child, initiator):
            if parent is child:
                raise ValueError("Cannot set a parent-child dependency on itself!")
            
            if child.status != Job.Status.STASHED:
                raise InvalidStatusException("The child job must be in the STASHED status.")
                
            if parent.super_id or child.super_id or parent.superjob or child.superjob:
                raise InvalidStatusException("A parent-child dependency can only be manually established/removed between top-level jobs.")
        
        @event.listens_for(cls.superjob, 'set', propagate=True)
        def _set_super_sub(subjob, superjob, old_superjob, initiator):
            if subjob.status != Job.Status.STASHED:
                raise InvalidStatusException("The subjob must be in the STASHED status.")
            
            # Superjob can be None when de-assigning
            if superjob:
                if superjob is subjob:
                    raise ValueError("Cannot set a super-sub dependency on itself!")
                
                if superjob.status != Job.Status.PROCESSING:
                    raise InvalidStatusException("The superjob must be in the PROCESSING status.")
    
    ###########################################################################
    # STATUS MUTATION                                                         #
    ###########################################################################
    
    def _ancestors(self):
        cls = self.__class__
        ancestors = []
        
        session = object_session(self)
        if session and session.bind.url.drivername == 'postgresql':
            # Needed to get super_id
            session.flush()
            
            l0 = literal_column('0').label('level')
            q_base = session.query(cls, l0).filter_by(
                 id = self.super_id
            ).cte(recursive = True)
            l1 = literal_column('level + 1').label('level')
            q_rec = session.query(cls, l1).filter(
                q_base.c.super_id == cls.id
            )
            q_cte = q_base.union_all(q_rec)
            
            ancestors = session.query(cls).select_entity_from(
                q_cte
            ).order_by(q_cte.c.level).all()
        
        else: # Fallback for any other backend
            job = self
            while job.superjob:
                ancestors.append(job.superjob)
                job = job.superjob
        
        return ancestors
    
    def _update_status(self):
        if not self.subjobs or self.status == Job.Status.DONE:
            return
        
        substatus = set([subjob.status for subjob in self.subjobs])
        
        if set([Job.Status.DONE]) >= substatus:
            # Need to run the epilog
            self._status = Job.Status.QUEUED
        
        elif set([
            Job.Status.DONE,
            Job.Status.FAILED,
        ]) >= substatus:
            self._status = Job.Status.FAILED
            self._ts_ended = func.now()
        
        else:
            self._status = Job.Status.PROCESSING
            if not self.ts_started:
                self._ts_started = func.now()
    
    def _submit(self):
        if self.subjobs:
            for subjob in self.subjobs:
                subjob._submit()
            self._update_status()
        elif self.status in [
            Job.Status.FAILED,
            Job.Status.STASHED,
        ]:
            self._status = Job.Status.QUEUED
            self._ts_queued = func.now()
    
    def submit(self):
        if self.status not in [
            Job.Status.FAILED,
            Job.Status.STASHED,
            Job.Status.PROCESSING,
        ]:
            raise InvalidStatusException("This job cannot be submitted in its current status.")
        
        self._submit()
        
        for ancestor in self._ancestors():
            ancestor._update_status()
    
    def _remove(self):
        session = object_session(self)
        if session:
            if self.subjobs:
                for subjob in self.subjobs:
                    subjob._remove()
                session.delete(self)
            elif self.status in [
                Job.Status.STASHED,
                Job.Status.FAILED,
            ]:
                session.delete(self)
        else:
            raise DetachedInstanceError()
    
    def remove(self):
        if self.superjob:
            raise InvalidStatusException("This job is not a top-level job and cannot be removed.")
        
        if self.status not in [
            Job.Status.FAILED,
            Job.Status.STASHED,
        ]:
            raise InvalidStatusException("This job cannot be removed in its current status.")
        
        if self.parents or self.children:
            raise InvalidStatusException("Cannot remove a linked job.")
        
        self._remove()
    
    def _abort(self):
        if self.subjobs:
            for subjob in self.subjobs:
                subjob._abort()
            self.update_status()
        elif self.status in [
            Job.Status.QUEUED,
            Job.Status.PROCESSING,
        ]:
            self.finish('Job was aborted due to user request.')
    
    def abort(self):
        if self.status not in [
            Job.Status.QUEUED,
            Job.Status.PROCESSING,
        ]:
            raise InvalidStatusException("This job cannot be aborted in its current status.")
        
        self._abort()
        
        for ancestor in self._ancestors():
            ancestor._update_status()
    
    def reset(self):
        if self.subjobs:
            raise InvalidStatusException("This job already has subjobs and cannot be returned to the stash.")
        
        if self.status not in [
            Job.Status.FAILED,
            Job.Status.QUEUED,
        ]:
            raise InvalidStatusException("This job cannot be returned to the stash in its current status.")
        
        self._status = Job.Status.STASHED
        self._ts_queued = None
        self._ts_started = None
        self._ts_ended = None
    
    def clone(self):
        job = Job(self.name, self.task)
        job._config   = self._config
        job._input    = self._input
        job.parents  = self.parents
        
        return job
    
    ###########################################################################
    # DATASET ACCESS AND MUTATION                                             #
    ###########################################################################
    
    def get_raw_dataset(self, dataset):
        if dataset not in ['config', 'input', 'output']:
            raise ValueError("The value '%s' is not a valid dataset." % dataset)
        attr = "raw_%s" % dataset
        return getattr(self, attr)
    
    def get_dataset(self, dataset):
        value = self.get_raw_dataset(dataset) or ''
        return yaml.safe_load(value)
    
    def assert_editable_dataset(self, dataset):
        if dataset in ['config', 'input']:
            if self.status != Job.Status.STASHED:
                raise InvalidStatusException("A Job's %s can only be modified when STASHED." % dataset)
        elif dataset in ['output']:
            if self.status != Job.Status.PROCESSING:
                raise InvalidStatusException("A Job's %s can only be modified when PROCESSING." % dataset)
        else:
            raise ValueError("The value '%s' is not a valid dataset." % dataset)
    
    def set_dataset(self, dataset, value):
        self.assert_editable_dataset(dataset)
        data = yaml.safe_dump(value, default_flow_style=False)
        attr = "_%s" % dataset
        setattr(self, attr, data)
    
    @contextlib.contextmanager
    def edit_dataset(self, dataset):
        self.assert_editable_dataset(dataset)
        value = self.get_dataset(dataset)
        yield value
        self.set_dataset(dataset, value)
    
    def get_config(self):
        return self.get_dataset('config')
    
    def get_input(self):
        return self.get_dataset('input')
    
    def get_output(self):
        return self.get_dataset('output')
    
    def set_config(self, value):
        self.set_dataset('config', value)
    
    def set_input(self, value):
        self.set_dataset('input', value)
    
    def edit_config(self):
        return self.edit_dataset('config')
    
    def edit_input(self):
        return self.edit_dataset('input')
    
    ###########################################################################
    # TASK                                                                    #
    ###########################################################################
    
    @_deprecated
    def _create_subjobs(self, subtasks):
        """
        {
            'subjobs' : [
                Task_A(config),
                Task_B(config),
                Task_B(config),
            ],
            'input' : {
                task_M : <input>,
                task_N : <input>,
            }
            'links' : [
                ( task_X, task_Y ),
            ]
        }
        """
        subjobs = {}
        for task in subtasks.get('subjobs', []):
            subjobs[task] = task.create_job()
            subjobs[task].set_config(task.config)
        
        for (task, inp) in subtasks.get('input', {}).iteritems():
            subjobs[task].set_input(inp)
        
        for link in subtasks.get('links', []):
            subjobs[link[0]].children.add(subjobs[link[1]])
        
        return set(subjobs.values())
    
    @_deprecated
    def _create_childjobs(self, childtasks):
        """
        {
            'children' : {
                Task_A(config),
                Task_B(config),
                Task_B(config),
            },
            'links' : [
                ( task_X, task_Y ),
            ]
            'output' : <output>
        }
        """
        children = {}
        for task in childtasks.get('children', []):
            children[task] = task.create_job()
            children[task].set_config = task.config
        
        for (task, inp) in childtasks.get('input', {}).iteritems():
            children[task].set_input(inp)
        
        for link in childtasks.get('links', []):
            children[link[0]].children.add(children[link[1]])
        
        return set(children.values())
    
    def assert_is_available(self):
        if not self.task:
            raise TaskNotAvailableException(self.name)
    
    def process(self, token=None):
        self.assert_is_available() # TODO: Strictly, it doesnt have to be called
        if token and self.status == Job.Status.PROCESSING:
            if self.tag.get(TAG_TOKEN, None) != token:
                raise TokenMismatchException("Incorrect token given for a reserved job.")
        elif self.status != Job.Status.QUEUED:
            raise InvalidStatusException("Only jobs in QUEUED status can be processed.")
        if any([parent.status != Job.Status.DONE for parent in self.parents]):
            raise InvalidStatusException("This job cannot be executed because not all of its parents have finished.")
        # Moving job into PROCESSING state
        self._status = Job.Status.PROCESSING
        self._ts_started = func.now()
        self._output = None
        # Store token for reserved jobs
        if token:
            self.tag[TAG_TOKEN] = token
        
        for ancestor in self._ancestors():
            ancestor._update_status()
    
    def prolog(self, token=None):
        self.assert_is_available()
        if self.status != Job.Status.PROCESSING:
            raise InvalidStatusException("Only jobs in PROCESSING status can be executed.")
        if self.tag.get(TAG_TOKEN, None) != token:
            raise TokenMismatchException("Incorrect token given for a reserved job.")
        if self.subjobs:
            raise InvalidStatusException("Cannot execute prolog on jobs that have subjobs.")
        # Execute prolog implementation
        subtasks =  self.task.prolog(self)
        if subtasks:
            subjobs = self._create_subjobs(subtasks)
            self.subjobs |= subjobs
            for job in subjobs:
                job.submit()
    
    def run(self, token=None):
        self.assert_is_available()
        if self.status != Job.Status.PROCESSING:
            raise InvalidStatusException("Only jobs in PROCESSING status can be executed.")
        if self.tag.get(TAG_TOKEN, None) != token:
            raise TokenMismatchException("Incorrect token given for a reserved job.")
        if self.subjobs:
            raise InvalidStatusException("Cannot execute run on jobs that have subjobs.")
        if self.raw_output:
            raise InvalidStatusException("Cannot execute a job that already has an output.")
        # Execute run implementation 
        self.set_dataset('output', self.task.run(self))
        self._status = Job.Status.DONE
    
    def epilog(self, token=None):
        self.assert_is_available()
        if self.status != Job.Status.PROCESSING:
            raise InvalidStatusException("Only jobs in PROCESSING status can be executed.")
        if self.tag.get(TAG_TOKEN, None) != token:
            raise TokenMismatchException("Incorrect token given for a reserved job.")
        if not self.subjobs:
            raise InvalidStatusException("Cannot execute epilog on jobs that have no subjobs.")
        # Execute epilog implementation
        value = self.task.epilog(self)
        if isinstance(value, dict) and 'children' in value and 'links' in value:
            # Create child jobs
            children = self._create_childjobs(value)
            self.children |= children
            for job in children:
                job.submit()
            
            if 'output' in value:
                self.set_dataset('output', value['output'])
            else:
                self.set_dataset('output', [
                    job.output
                    for job in self.subjobs
                    if not job.children
                ])
        else:
            self.set_dataset('output', value)
        self._status = Job.Status.DONE
    
    def finish(self, tb=None):
        if tb:
            if self.status != Job.Status.PROCESSING:
                raise InvalidStatusException("Only jobs in PROCESSING status can be finished with error.")
            
            self._status = Job.Status.FAILED
            self.tag[TAG_TRACEBACK] = tb
        
        elif self.status != Job.Status.DONE:
            raise InvalidStatusException("Only jobs in PROCESSING status can be finished with error.")
            
            self.tag.pop(TAG_TRACEBACK, None)
        
        self._ts_ended = func.now()
        self.tag.pop(TAG_TOKEN, Tag())
        
        for ancestor in self._ancestors():
            ancestor._update_status()

class Tag(Base):
    __tablename__ = 'tag'
    __table_args__ = (
        # Primary key
        PrimaryKeyConstraint('job_id', 'name', name = 'pk_tag'),
        # Foreign keys
        ForeignKeyConstraint(['job_id'], ['job.id'], onupdate='CASCADE', ondelete='CASCADE', name='fk_tag_job'),
        # Indexes
        Index('ix_tag_name_value', 'name', 'value'),
    )
    
    # Columns
    _job_id = Column('job_id', Integer,    nullable=False)
    _name   = Column('name',   String(20), nullable=False)
    _value  = Column('value',  Text,       nullable=True)
    
    @hybrid_property
    def job_id(self):
        return self._job_id
    
    @hybrid_property
    def name(self):
        return self._name
    
    @hybrid_property
    def value(self):
        return self._value
    
    def __repr__(self):
        return u"%s(job_id=%s, name=%s, value=%s)" % (
            self.__class__.__name__,
            repr(self._job_id),
            repr(self._name),
            repr(self._value),
        )
