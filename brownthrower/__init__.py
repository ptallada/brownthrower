#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""\
Brownthrower main module documentation
"""

import contextlib
import logging
import textwrap
import traceback
import yaml

from sqlalchemy import event, func, literal_column
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship, reconstructor
from sqlalchemy.orm.collections import attribute_mapped_collection
from sqlalchemy.orm.session import object_session
from sqlalchemy.schema import (Column, ForeignKeyConstraint, Index,
                               PrimaryKeyConstraint, UniqueConstraint)
from sqlalchemy.sql import functions
from sqlalchemy.types import DateTime, Integer, String, Text

from . import release
from . import taskstore
from .model import Base, create_engine, is_serializable_error, retry_on_serializable_error, transactional_session

try:
    from logging import NullHandler
except ImportError:
    from logutils import NullHandler

log = logging.getLogger('brownthrower')
log.addHandler(NullHandler())

tasks = taskstore.TaskStore()
"""Global task container, implemented as a read-only dict."""

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
    _super_id      = Column('super_id',      Integer, nullable=True)
    _parent_job_id = Column('parent_job_id', Integer, nullable=False)
    _child_job_id  = Column('child_job_id',  Integer, nullable=False)
    
    def __repr__(self):
        return u"%s(super_id=%s, parent_job_id=%s, child_job_id=%s)" % (
            self.__class__.__name__,
            repr(self._super_id),
            repr(self._parent_job_id),
            repr(self._child_job_id),
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
    
    def __init__(self, task):
        self.message = "Task '%s' is not available in this environment." % task
        
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
    
    _id         = Column('id',         Integer,    nullable=False)
    _super_id   = Column('super_id',   Integer,    nullable=True)
    _task       = Column('task',       String(50), nullable=False)
    _status     = Column('status',     String(20), nullable=False)
    _config     = Column('config',     Text,       nullable=True)
    _input      = Column('input',      Text,       nullable=True)
    _output     = Column('output',     Text,       nullable=True)
    _ts_created = Column('ts_created', DateTime,   nullable=False, default=functions.now())
    _ts_queued  = Column('ts_queued',  DateTime,   nullable=True)
    _ts_started = Column('ts_started', DateTime,   nullable=True)
    _ts_ended   = Column('ts_ended',   DateTime,   nullable=True)
    
    ###########################################################################
    # RELATIONSHIPS                                                           #
    ###########################################################################
    
    parents  = relationship('Job',
        back_populates    = 'children', secondary = 'dependency',
        primaryjoin       = 'Dependency._child_job_id == Job._id',
        secondaryjoin     = 'Job._id == Dependency._parent_job_id',
        collection_class  = set)
    """parents relationship"""
    
    children = relationship('Job',
        back_populates    = 'parents',  secondary = 'dependency',
        primaryjoin       = 'Dependency._parent_job_id == Job._id',
        secondaryjoin     = 'Job._id == Dependency._child_job_id',
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
        statsu class
        """
        STASHED     = 'STASHED'
        """Preparation phase. It is being configured and cannot be executed yet."""
        QUEUED      = 'QUEUED'
        """\
        The job has been configured and its dependencies are already set.
        It will be executed as soon as possible.
        """
        PROCESSING  = 'PROCESSING'
        """The job is being processed."""
        DONE        = 'DONE'
        """The job has finished successfully."""
        FAILED      = 'FAILED'
        """The job did not finish successfully."""
    
    ###########################################################################
    # CONSTRUCTORS AND SPECIAL METHODS                                        #
    ###########################################################################
    
    def __init__(self, task, impl = None):
        values = {
            '_task' : task,
            '_status' : Job.Status.STASHED,
            '_ts_created' : func.now(),
        }
          
        super(Job, self).__init__(**values)
        self._reconstruct()
        
        if impl:
            if impl._bt_name != task:
                raise ValueError("Mismatch between task name and implementer class.")
            if self._impl and self._impl != impl:
                raise ValueError("Mismatch between internal implementer task and the provided one.")
            
            self.set_dataset('config', impl.config_sample())
            self.set_dataset('input', impl.input_sample())
    
    @reconstructor
    def _reconstruct(self):
        self._impl = tasks.get(self.task, None)
    
    def __repr__(self):
        return u"%s(id=%s, super_id=%s, task=%s, status=%s)" % (
            self.__class__.__name__,
            repr(self._id),
            repr(self._super_id),
            repr(self._task),
            repr(self._status),
        )
    
    ###########################################################################
    # DESCRIPTORS                                                             #
    ###########################################################################
    
    @hybrid_property
    def id(self):
        return self._id
    
    @hybrid_property
    def super_id(self):
        return self._super_id
    
    @hybrid_property
    def task(self):
        return self._task
    
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
            self.update_status()
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
        # FIXME: We sould not be using session here...
        session = object_session(self)
        if self.subjobs:
            for subjob in self.subjobs:
                subjob._remove()
            session.delete(self)
        elif self.status in [
            Job.Status.STASHED,
            Job.Status.FAILED,
        ]:
            session.delete(self)
    
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
    
    def _cancel(self):
        if self.subjobs:
            for subjob in self.subjobs:
                subjob._cancel()
            self._update_status()
        else:
            raise NotImplementedError
    
    def cancel(self):
        if self.status not in [
            Job.Status.PROCESSING,
        ]:
            raise InvalidStatusException("This job cannot be cancelled in its current status.")
        
        self._cancel()
        
        for ancestor in self._ancestors():
            ancestor._update_status()
    
    def reset(self):
        if self.superjob:
            raise InvalidStatusException("This job is not a top-level job and cannot be returned to the stash.")
        
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
        job = Job(self.task, self._impl)
        job._config   = self._config
        job._input    = self._input
        job.parents  = self.parents
        job.children = self.children
        
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
        self.edit_dataset('config')
    
    def edit_input(self):
        self.edit_dataset('input')
    
    ###########################################################################
    # TASK                                                                    #
    ###########################################################################
    
    def get_sample(self, dataset):
        if dataset not in ['config', 'input']:
            raise ValueError("The value '%s' is not a valid dataset." % dataset)
        
        meth = '%s_sample' % dataset
        if self._impl:
            return getattr(self._impl, meth)()
        else:
            return ''
    
    def assert_is_available(self):
        if not self._impl:
            raise TaskNotAvailableException(self.task)
    
    def process(self):
        self.assert_is_available() # TODO: Strictly, it doesnt have to be called
        if self.status != Job.Status.QUEUED:
            raise InvalidStatusException("Only jobs in QUEUED status can be processed.")
        if any([parent.status != Job.Status.DONE for parent in self.parents]):
            raise InvalidStatusException("This job cannot be executed because not all of its parents have finished.")
        # Moving job into PROCESSING state
        self._status = Job.Status.PROCESSING
        self._ts_started = func.now()
        self._output = None
        for ancestor in self._ancestors():
            ancestor._update_status()
    
    def prolog(self):
        self.assert_is_available()
        if self.status != Job.Status.PROCESSING:
            raise InvalidStatusException("Only jobs in PROCESSING status can be executed.")
        if self.subjobs:
            raise InvalidStatusException("Cannot execute prolog on jobs that have subjobs.")
        # Execute prolog implementation
        return self._impl.prolog(self)
    
    def run(self):
        self.assert_is_available()
        if self.status != Job.Status.PROCESSING:
            raise InvalidStatusException("Only jobs in PROCESSING status can be executed.")
        if self.subjobs:
            raise InvalidStatusException("Cannot execute run on jobs that have subjobs.")
        if self.raw_output:
            raise InvalidStatusException("Cannot execute a job that already has an output.")
        # Execute run implementation 
        value = self._impl.run(self)
        self.set_dataset('output', value)
        return value
    
    def epilog(self):
        self.assert_is_available()
        if self.status != Job.Status.PROCESSING:
            raise InvalidStatusException("Only jobs in PROCESSING status can be executed.")
        if not self.subjobs:
            raise InvalidStatusException("Cannot execute epilog on jobs that have no subjobs.")
        # Execute epilog implementation
        (children, value) = self._impl.epilog(self)
        self.set_dataset('output', value)
        return children
    
    def finish(self, exc):
        if self.status != Job.Status.PROCESSING:
            raise InvalidStatusException("Only jobs in PROCESSING status can be finished.")
        self._ts_ended = func.now()
        if not exc:
            self._status = Job.Status.DONE
        else:
            self._status = Job.Status.FAILED
            self.tag['last_traceback'] = traceback.format_exc()
        
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
    
    def __repr__(self):
        return u"%s(job_id=%s, name=%s, value=%s)" % (
            self.__class__.__name__,
            repr(self._job_id),
            repr(self._name),
            repr(self._value),
        )

class DocumentedTask(type):
    @property
    def summary(self):
        return self.__doc__.strip().split('\n')[0].strip()
    
    @property
    def description(self):
        return textwrap.dedent('\n'.join(self.__doc__.strip().split('\n')[1:])).strip()

class Task(object):
    """
    Base class for user-defined Tasks.
    
    All Task subclasses must inherit from this class and override, as needed,
    the following methods:
     * :meth:`prolog`
     * :meth:`epilog`
     * :meth:`run`
    """
    __metaclass__ = DocumentedTask
    
    _bt_name = None
    
    @classmethod
    def create_job(cls):
        """\
        Create a :class:`Job` instance corresponding to this Task.
        """
        return Job(task = cls._bt_name, impl = cls)
    
    @classmethod
    def prolog(cls, job):
        """
        Prepares this Task for execution.
        
        Return a mapping with the following structure, or None if no subjobs are
        required:
        
            {
                'subjobs' : {
                    Task_A(config),
                    Task_B(config),
                    Task_B(config),
                },
                'input' : {
                    task_M : <input>,
                    task_N : <input>,
                }
                'links' : [
                    ( task_X, task_Y ),
                ]
            }
        
        The 'subjobs' entry is a set, with each element being a Task instance
        with its associated config.
        
        The 'input' entry is also a mapping, indexed by any of the keys present
        in the 'subjobs' dictionary, and its value is the input of that Task.
        
        Finally, the 'links' entry contains a list of tuples. Each tuple
        contains two Task instances (which must be present in the 'subjobs'
        mapping) representing the parent and the child sides, respectively, of a
        parent-child dependency.
        
        @param job: corresponding job for this task
        @type job: brownthrower.Job
        @return: a mapping representing the structure of the subjobs
        @rtype: mapping
        """
        pass
    
    @classmethod
    def epilog(cls, job):
        """
        Wraps up this Task for the end of its processing. When this method is
        called, it can safely assume that the 'config' and 'out' parameters have
        been checked previously and that they are both valid.
        
        Return a mapping with the following structure:
        
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
        
        The 'children' entry is a set, with each key being a Task instance
        with its associated config.
        
        The 'links' entry contains a list of tuples. Each tuple contains two
        Task instances (which must be present in the 'children' mapping)
        representing the parent and the child sides, respectively, of a parent-
        child dependency.
        
        Finally, the 'output' entry contains the final output of this Job.
        
        @param config: mapping with the required configuration values
        @type config: dict
        @param out:  mapping with the output of the leaf Jobs
        @type out: dict
        @param job_id: unique identifier for this job
        @type job_id: int
        @return: a mapping with the output and the structure of the child jobs
        @rtype: dict
        """
        pass
    
    @classmethod
    def run(cls, job):
        """
        Executes this Task. When this method is called, it can safely assume
        that the 'inp' parameter has been checked previously and that it is
        valid.
        
        @param inp:  list with the output of the parent jobs
        @type inp: list
        @param job_id: unique identifier for this job
        @type job_id: int
        @return: output to be delivered as input for child jobs
        """
        pass
    
    @classmethod
    def config_sample(cls):
        """
        Return a working configuration sample, that may be used as default.
        
        Users are expected to use these settings and only override those that do
        not fit their needs.
        
        @rtype: object
        """
        return None
    
    @classmethod
    def input_sample(cls):
        """
        Return a working input sample, that may be used as default.
        
        @rtype: object
        """
        return None
