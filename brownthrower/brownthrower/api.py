#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

from sqlalchemy import event, func, literal_column
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship, reconstructor
from sqlalchemy.orm.attributes import manager_of_class
from sqlalchemy.orm.collections import attribute_mapped_collection
from sqlalchemy.orm.session import object_session

from . import model

log = logging.getLogger('brownthrower.api')

Base = declarative_base()

class InvalidStatusException(Exception):
    def __init__(self, message=None):
        self.message = message
        
    def __str__(self):
        return str(self.message)

class Job(Base, model.Job):
    """
    Base class for user-defined Jobs.
    
    All Job subclasses must inherit from this class and override the methods
    defined in brownthrower.interface.Job.
    """
    
    class Status(object):
        # Preparation phase. It is being configured and cannot be executed yet.
        STASHED     = 'STASHED'
        # The job has been configured and its dependencies are already set.
        # It will be executed as soon as possible.
        QUEUED      = 'QUEUED'
        # The user has asked to cancel this job.
        CANCELLING  = 'CANCELLING'
        # The job has been interrupted. No inner job has failed.
        CANCELLED   = 'CANCELLED'
        # The job is being processed.
        PROCESSING  = 'PROCESSING'
        # The job has finished successfully.
        DONE        = 'DONE'
        # The job is still being processed and some inner job has failed.
        FAILING     = 'FAILING'
        # The job did not finish succesfully.
        FAILED      = 'FAILED'
    
    # Relationships
    parents  = relationship('api.Job',
        back_populates    = 'children', secondary = 'dependency',
        primaryjoin       = 'Dependency._child_job_id == api.Job._id',
        secondaryjoin     = 'api.Job._id == Dependency._parent_job_id',
        collection_class  = set)
    children = relationship('api.Job',
        back_populates    = 'parents',  secondary = 'dependency',
        primaryjoin       = 'Dependency._parent_job_id == api.Job._id',
        secondaryjoin     = 'api.Job._id == Dependency._child_job_id',
        collection_class  = set)
    superjob = relationship('api.Job',
        back_populates    = 'subjobs',
        primaryjoin       = 'api.Job._super_id == api.Job._id',
        remote_side       = 'api.Job._id')
    subjobs  = relationship('api.Job',
        back_populates    = 'superjob',
        primaryjoin       = 'api.Job._super_id == api.Job._id',
        cascade           = 'all, delete-orphan', passive_deletes = True,
        collection_class  = set)
    tags     = relationship('api.Tag',
        cascade          = 'all, delete-orphan', passive_deletes = True,
        collection_class = attribute_mapped_collection('_name'))
    
    # Proxies
    tag = association_proxy('tags', '_value', creator=lambda name, value: Tag(_name=name, _value=value))
    
    def __init__(self, *args, **kwargs):
        raise NotImplementedError
    
    @classmethod
    def _init(cls, task):
        values = {
            '_task' : task,
            '_status' : Job.Status.STASHED,
            '_ts_created' : func.now(),
        }
          
        job = manager_of_class(cls).new_instance()
        super(Job, job).__init__(**values)
         
        job._impl = None 
        job._reconstruct()
         
        return job
    
    @reconstructor
    def _reconstruct(self):
        # TODO: self._impl = <search Task in task store>
        pass
    
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
    def config(self):
        return self._config
    
    @hybrid_property
    def input(self):
        return self._input
    
    @hybrid_property
    def output(self):
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
            Job.Status.CANCELLED,
        ]) >= substatus:
            self._status = Job.Status.CANCELLED
            self._ts_ended = func.now()
        
        elif set([
            Job.Status.DONE,
            Job.Status.CANCELLED,
            Job.Status.FAILED,
        ]) >= substatus:
            self._status = Job.Status.FAILED
            self._ts_ended = func.now()
        
        elif set([
            Job.Status.DONE,
            Job.Status.CANCELLED,
            Job.Status.FAILED,
            Job.Status.CANCELLING,
        ]) >= substatus:
            self._status = Job.Status.CANCELLING
        
        elif set([
            Job.Status.DONE,
            Job.Status.CANCELLED,
            Job.Status.CANCELLING,
            Job.Status.PROCESSING,
            Job.Status.QUEUED,
        ]) >= substatus:
            self._status = Job.Status.PROCESSING
            if not self.ts_started:
                self._ts_started = func.now()
        else:
            self._status = Job.Status.FAILING
    
    def _submit(self):
        if self.subjobs:
            for subjob in self.subjobs:
                subjob._submit()
            self.update_status()
        elif self.status in [
            Job.Status.CANCELLED,
            Job.Status.FAILED,
            Job.Status.STASHED,
        ]:
            self._status = Job.Status.QUEUED
            self._ts_queued = func.now()
    
    def submit(self):
        if self.status not in [
            Job.Status.CANCELLED,
            Job.Status.FAILED,
            Job.Status.STASHED,
        ]:
            raise InvalidStatusException("This job cannot be submitted in its current status.")
        
        self._submit()
        
        for ancestor in self._ancestors():
            ancestor._update_status()
    
    def _remove(self):
        session = object_session(self)
        if self.subjobs:
            for subjob in self.subjobs:
                subjob._remove()
            session.delete(self)
        elif self.status in [
            Job.Status.STASHED,
            Job.Status.FAILED,
            Job.Status.CANCELLED,
        ]:
            session.delete(self)
    
    def remove(self):
        if self.superjob:
            raise InvalidStatusException("This job is not a top-level job and cannot be removed.")
        
        if self.status not in [
            Job.Status.CANCELLED,
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
        elif self.status == Job.Status.QUEUED:
            self._status  = Job.Status.CANCELLED
            self._ts_ended = func.now()
        elif self.status == Job.Status.PROCESSING:
            self._status  = Job.Status.CANCELLING
    
    def cancel(self):
        if self.status not in [
            Job.Status.FAILING,
            Job.Status.PROCESSING,
            Job.Status.QUEUED,
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
            Job.Status.CANCELLED,
            Job.Status.FAILED,
            Job.Status.QUEUED,
        ]:
            raise InvalidStatusException("This job cannot be returned to the stash in its current status.")
        
        self._status = Job.Status.STASHED
        self._ts_queued = None
        self._ts_started = None
        self._ts_ended = None
    
    def clone(self):
        job = self._init(self.task)
        job._config   = self.config
        job._input    = self.input
        job._parents  = self.parents
        job._children = self.children
        
        return job
    
#     def _prolog(self):
#         if self._impl:
#             return self._impl.prolog(self)
#         else:
#             raise NotImplementedError
#     
#     def _run(self):
#         if self._impl:
#             return self._impl.run(self)
#         else:
#             raise NotImplementedError
#     
#     def _epilog(self):
#         if self._impl:
#             return self._impl.epilog(self)
#         else:
#             raise NotImplementedError

class Dependency(Base, model.Dependency):
    pass

class Tag(Base, model.Tag):
    pass
    
def create_engine(db_url):
    engine = model.create_engine(db_url)
    
    Base.metadata.create_all(bind = engine)
    
    return engine
