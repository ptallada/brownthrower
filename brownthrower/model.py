#!/usr/bin/env python
# -*- coding: utf-8 -*-

import contextlib
import copy
import logging
import sys
import traceback
import yaml

from sqlalchemy import event, func, literal_column
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship, reconstructor, deferred
from sqlalchemy.orm.collections import attribute_mapped_collection
from sqlalchemy.orm.exc import DetachedInstanceError
from sqlalchemy.orm.session import object_session
from sqlalchemy.schema import ForeignKeyConstraint, Index, PrimaryKeyConstraint, UniqueConstraint
from sqlalchemy.sql import functions
from sqlalchemy.sql.expression import literal
from sqlalchemy.types import DateTime, Integer, String, Text

from . import taskstore
from . import utils
from .base import Column, Base

log = logging.getLogger('brownthrower.model')

tasks = taskstore.TaskStore()
"""Global task container, implemented as a read-only dict."""

TAG_TRACEBACK = 'bt_traceback'

class Dependency(Base):
    """\
    Parent-child dependencies between jobs.
    """
    
    __tablename__ = 'dependency'
    __table_args__ = (
        # Primary key
        PrimaryKeyConstraint('parent_id', 'child_id', name='pk_dependency'),
        # Foreign keys
        # FIXME: This constraints are not useful for top-level jobs
        ForeignKeyConstraint(            ['parent_id'],                 ['job.id'], onupdate='CASCADE', ondelete='CASCADE', name= 'fk_dependency_parent'),
        ForeignKeyConstraint(            ['child_id'],                  ['job.id'], onupdate='CASCADE', ondelete='CASCADE', name= 'fk_dependency_child'),
        ForeignKeyConstraint(['super_id', 'parent_id'], ['job.super_id', 'job.id'], onupdate='CASCADE', ondelete='CASCADE', name= 'fk_dependency_super_parent'),
        ForeignKeyConstraint(['super_id', 'child_id'],  ['job.super_id', 'job.id'], onupdate='CASCADE', ondelete='CASCADE', name= 'fk_dependency_super_child'),
    )
    
    # Columns
    _super_id  = Column('super_id',  Integer, nullable=True,  comment="super job ID.")
    _parent_id = Column('parent_id', Integer, nullable=False, comment="parent job ID.")
    _child_id  = Column('child_id',  Integer, nullable=False, comment="child job ID.")
    
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

class InvalidChildOrSubjobException(Exception):
    """\
    Raised when an element on new_children or new_subjobs has an id or super_id.
    """
    
    def __init__(self, message=None):
        self.message = message
        
    def __str__(self):
        return str(self.message)

class Job(Base):
    """
    All jobs in under BT domain are stored here.
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
        Index('ix_job_name',   'name'),
    )
    
    ###########################################################################
    # COLUMNS                                                                 #
    ###########################################################################
    
    _id          =          Column('id',          Integer,    nullable=False, comment="unique identifier (ID)")
    _super_id    =          Column('super_id',    Integer,    nullable=True,  comment="super job ID")
    _name        =          Column('name',        String(50), nullable=False, comment="short name for categorization")
    _status      =          Column('status',      String(20), nullable=False, comment="current status")
    _description = deferred(Column('description', Text,       nullable=False, comment="user description", server_default=''), group='desc')
    _token       =          Column('token',       String(32), nullable=True,  comment="unique value for reservations")
    _config      = deferred(Column('config',      Text,       nullable=True,  comment="configuration data (in YAML format)"), group='yaml')
    _input       = deferred(Column('input',       Text,       nullable=True,  comment="input data (in YAML format)"),         group='yaml')
    _output      = deferred(Column('output',      Text,       nullable=True,  comment="output data (in YAML format)"),        group='yaml')
    _ts_created  =          Column('ts_created',  DateTime,   nullable=False, comment="when was this job created (UTC)", default=functions.now())
    _ts_queued   =          Column('ts_queued',   DateTime,   nullable=True,  comment="when was this job submitted for execution (UTC)")
    _ts_started  =          Column('ts_started',  DateTime,   nullable=True,  comment="when did this job start executing (UTC)")
    _ts_ended    =          Column('ts_ended',    DateTime,   nullable=True,  comment="when did this job finish executing (UTC)")
    
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
    
    _superjob = relationship('Job',
        back_populates    = '_subjobs',
        primaryjoin       = 'Job._super_id == Job._id',
        remote_side       = 'Job._id')
    """superjob relationship"""
    
    _subjobs  = relationship('Job',
        back_populates    = '_superjob',
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
        All the different values a job status can be.
        """
        STASHED = 'STASHED'
        """Preparation phase. It is being configured and cannot be executed yet."""
        QUEUED = 'QUEUED'
        """\
        The job has been configured and its dependencies are already set.
        It will be executed as soon as possible.
        """
        STAND_BY = 'STAND-BY'
        """Waiting its subjobs to finish."""
        RUNNING = 'RUNNING'
        """Being executed right now."""
        DONE = 'DONE'
        """Finished successfully."""
        FAILED = 'FAILED'
        """Finished with an error condition."""
    
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
        
        self._ro_subjobs = None
        
        self.new_children = set()
        self.new_subjobs  = set()
    
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
    def description(self):
        return self._description
    
    @description.setter
    def description(self, description):
        self._description = description
    
    @hybrid_property
    def token(self):
        return self._token
    
    @hybrid_property
    def superjob(self):
        return self._superjob
    
    @superjob.expression
    def superjob(self):
        return self._superjob
    
    @hybrid_property
    def subjobs(self):
        if self._ro_subjobs == None:
            self._ro_subjobs = utils.InmutableSet(self._subjobs)
        
        return self._ro_subjobs
    
    @subjobs.expression
    def subjobs(self):
        return self._subjobs
    
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
            
            if child.status not in [
                Job.Status.STASHED,
                Job.Status.QUEUED,
                Job.Status.FAILED,
            ]:
                raise InvalidStatusException("Cannot add or remove a child job if it is not in STASHED|QUEUED|FAILED state.")
            
            if child.subjobs:
                raise InvalidStatusException("Cannot add or remove a child job with subjobs.")
    
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
            self._status = Job.Status.STAND_BY
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
            Job.Status.STAND_BY,
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
        if self.status not in [
            Job.Status.FAILED,
            Job.Status.STASHED,
        ]:
            raise InvalidStatusException("This job cannot be removed in its current status.")
        
        if self.superjob:
            raise InvalidStatusException("This job is not a top-level job and cannot be removed.")
        
        if self.parents or self.children:
            raise InvalidStatusException("Cannot remove a linked job.")
        
        self._remove()
    
    def _abort(self):
        if self.subjobs:
            for subjob in self.subjobs:
                subjob._abort()
            self._update_status()
        
        if self.status in [
            Job.Status.QUEUED,
            Job.Status.RUNNING,
        ]:
            self._cleanup('Job was aborted due to user request.')
    
    def abort(self):
        if self.status not in [
            Job.Status.QUEUED,
            Job.Status.RUNNING,
            Job.Status.STAND_BY,
        ]:
            raise InvalidStatusException("This job cannot be aborted in its current status.")
        
        self._abort()
        
        for ancestor in self._ancestors():
            ancestor._update_status()
    
    def _reset(self):
        if self.status in [
            Job.Status.FAILED,
            Job.Status.QUEUED,
        ]:
            self._status = Job.Status.STASHED
            self._ts_queued = None
            self._ts_started = None
            self._ts_ended = None
            self._token = None
    
    def reset(self):
        if self.status not in [
            Job.Status.FAILED,
            Job.Status.QUEUED,
        ]:
            raise InvalidStatusException("This job cannot be returned to the stash in its current status.")
        
        if self.subjobs:
            raise InvalidStatusException("This job already has subjobs and cannot be returned to the stash.")
        
        self._reset()
    
    def clone(self):
        job = Job(self.name, self.task)
        job._config = copy.deepcopy(self._config)
        job._input  = copy.deepcopy(self._input)
        job.parents = self.parents.copy()
        
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
            if self.status != Job.Status.RUNNING:
                raise InvalidStatusException("A Job's %s can only be modified when RUNNING." % dataset)
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
    
    def assert_is_available(self):
        if not self.task:
            raise TaskNotAvailableException(self.name)
    
    def reserve(self, token):
        if not self.token:
            self._token = token
        
        if self.token != token:
            raise TokenMismatchException("Incorrect token given for this job.")
    
    def _start(self, token):
        self.reserve(token)
        
        if self.status == Job.Status.RUNNING:
            return
        
        if self.status != Job.Status.QUEUED:
            raise InvalidStatusException("Only jobs in QUEUED status can be reserved.")
        
        if any([parent.status != Job.Status.DONE for parent in self.parents]):
            raise InvalidStatusException("This job cannot be processed because not all of its parents have finished.")
        
        # Moving job into RUNNING state
        self._status = Job.Status.RUNNING
        self._ts_started = func.now()
        self._output = None
        
        for ancestor in self._ancestors():
            ancestor._update_status()
    
    def _run(self, token, debug):
        def validate_new_jobs(jobs):
            for job in jobs:
                if job.superjob or job.super_id or job.id:
                    raise InvalidChildOrSubjobException("New job %s is invalid." % (job))
                if job.status not in [Job.Status.STASHED, Job.Status.QUEUED]:
                    raise InvalidStatusException("New job %s has incorrect status." % (job))
        
        self.assert_is_available()
        
        if self.token != token:
            raise TokenMismatchException("Incorrect token given for this job.")
        
        if self.status != Job.Status.RUNNING:
            raise InvalidStatusException("Only jobs in RUNNING state can be executed.")
        
        new_state = {}
        try:
            if not self.subjobs:
                # PROLOG
                if debug:
                    utils.start_debugger(**debug)
                self.task.prolog(self)
                validate_new_jobs(self.new_subjobs)
                # RUN
                if not self.new_subjobs:
                    if debug:
                        utils.start_debugger(**debug)
                    new_state['output'] = self.task.run(self)
                    new_state['status'] = Job.Status.DONE
                else:
                    new_state['subjobs'] = self.new_subjobs
                    new_state['status'] = Job.Status.STAND_BY
            else:
                # EPILOG
                if debug:
                    utils.start_debugger(**debug)
                new_state['output'] = self.task.epilog(self)
                new_state['children'] = self.new_children
                new_state['status'] = Job.Status.DONE
                validate_new_jobs(self.new_children)
        
        except BaseException:
            try:
                raise
            except Exception:
                pass
            finally:
                new_state['traceback'] = ''.join(traceback.format_exception(*sys.exc_info()))
        finally:
            return new_state
    
    def _finish(self, token, new_state):
        if self.token != token:
            raise TokenMismatchException("Incorrect token given for this job.")
        
        tb = new_state.get('traceback', None)
        
        # AN ERROR OCCURRED
        if tb:
            self._cleanup(tb)
            return
        
        if 'output' in new_state:
            self.set_dataset('output', new_state.get('output', None))
        
        children = new_state.get('children', set())
        self.children |= children
        
        subjobs = new_state.get('subjobs', set())
        self._subjobs |= subjobs
        
        status = new_state.get('status', Job.Status.FAILED)
        self._status = status
        
        self._cleanup()
    
    def _cleanup(self, tb=None):
        if tb:
            self._status = Job.Status.FAILED
            self.tag[TAG_TRACEBACK] = tb
        
        else:
            self.tag.pop(TAG_TRACEBACK, Tag())
        
        self._ts_ended = func.now()
        self._token = None
        
        for ancestor in self._ancestors():
            ancestor._update_status()
    
    def cleanup(self, token, tb=None):
        if self.token != token:
            raise TokenMismatchException("Incorrect token given for this job.")
        
        self._cleanup(tb)
    
    ###########################################################################
    # TASK                                                                    #
    ###########################################################################
    
    @classmethod
    def _name_like(cls, patterns):
        if not patterns:
            return literal(True)
        
        crit = literal(False)
        for pattern in patterns:
            pattern = pattern.replace(r'\\', r'\\\\')
            pattern = pattern.replace(r'_',  r'\_')
            pattern = pattern.replace(r'%',  r'\%')
            pattern = pattern.replace(r'*',  r'%')
            pattern = pattern.replace(r'?',  r'_')
            crit |= (cls._name.like(pattern))
        
        return crit

class Tag(Base):
    """\
    Arbitrary key-value data associated with a job.
    """
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
    _job_id = Column('job_id', Integer,    nullable=False, comment="job ID")
    _name   = Column('name',   String(20), nullable=False, comment="unique name")
    _value  = Column('value',  Text,       nullable=True,  comment="data associated")
    
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
