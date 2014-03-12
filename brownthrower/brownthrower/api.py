#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

from sqlalchemy import event, func, literal_column
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship
from sqlalchemy.orm.collections import attribute_mapped_collection
from sqlalchemy.orm.session import object_session

from brownthrower import interface, model

log = logging.getLogger('brownthrower.api')

Base = declarative_base()

class InvalidStatusException(Exception):
    def __init__(self, message=None):
        self.message = message
        
    def __str__(self):
        return str(self.message)

class Job(Base, model.Job, interface.Job):
    
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
        back_populates   = 'children', secondary = 'dependency',
        primaryjoin      = 'Dependency._child_job_id == api.Job._id',
        secondaryjoin    = 'api.Job._id == Dependency._parent_job_id')
    children = relationship('api.Job',
        back_populates   = 'parents',  secondary = 'dependency',
        primaryjoin      = 'Dependency._parent_job_id == api.Job._id',
        secondaryjoin    = 'api.Job._id == Dependency._child_job_id')
    superjob = relationship('api.Job',
        back_populates   = 'subjobs',
        primaryjoin      = 'api.Job._super_id == api.Job._id',
        remote_side      = 'api.Job._id')
    subjobs  = relationship('api.Job',
        back_populates   = 'superjob',
        primaryjoin      = 'api.Job._super_id == api.Job._id',
        cascade          = 'all, delete-orphan', passive_deletes = True)
    tags     = relationship('api.Tag',
        back_populates   = 'job',
        collection_class = attribute_mapped_collection('name'),
        cascade          = 'all, delete-orphan', passive_deletes = True)
    
    # Proxies
    tag = association_proxy('tags', 'value', creator=lambda name, value: Tag(name=name, value=value))
    
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
        
        @event.listens_for(cls.children, 'append')
        @event.listens_for(cls.children, 'remove')
        def _set_parent_children(parent, child, initiator):
            print "_set_parent_children"
            parent_session = object_session(parent)
            child_session  = object_session(child)
            
            if parent_session:
                parent_session.flush()
                parent_session.refresh(parent, lockmode='read')
            
            if child_session:
                child_session.flush()
                child_session.refresh(child, lockmode='read')
            
            if parent is child:
                raise ValueError("Cannot set a parent-child dependency on itself!")
            
            if child.status != Job.Status.STASHED:
                raise InvalidStatusException("The child job must be in the STASHED status.")
                
            if parent.super_id or child.super_id or parent.superjob or child.superjob:
                raise InvalidStatusException("A parent-child dependency can only be manually established/removed between top-level jobs.")
        
        @event.listens_for(cls.superjob, 'set')
        def _set_super_sub(subjob, superjob, old_superjob, initiator):
            print "_set_super_sub"
            subjob_session   = object_session(subjob)
            
            if subjob_session:
                subjob_session.flush()
                subjob_session.refresh(subjob, lockmode='read')
            
            if subjob.status != Job.Status.STASHED:
                raise InvalidStatusException("The subjob must be in the STASHED status.")
            
            # Superjob can be None when de-assigning
            if superjob:
                superjob_session = object_session(superjob)
                
                if superjob_session:
                    superjob_session.flush()
                    superjob_session.refresh(superjob, lockmode='read')
                
                if superjob.status != Job.Status.PROCESSING:
                    raise InvalidStatusException("The superjob must be in the PROCESSING status.")
    
    def _ancestors(self, lockmode=False):
        cls = self.__class__
        ancestors = []
        
        session = object_session(self)
        
        if session and session.bind.url.drivername == 'postgresql':
            l0 = literal_column('0').label('level')
            q_base = session.query(cls, l0).filter_by(
                 id = self.id
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
            ancestors.append(job)
            while job.superjob:
                ancestors.append(job.superjob)
                job = job.superjob
            return ancestors
        
        if session:
            # Expire all affected instances and reload them already locked
            session.flush()
            ids = [job.id for job in ancestors]
            map(session.expire, ancestors)
            _ = session.query(cls).filter(
                cls.id.in_(ids)
            ).with_lockmode(lockmode).all()
            
            # Check that no entry was deleted in the middle
            map(session.query(cls).get, ids)
        
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
        ancestors = self._ancestors(lockmode='update')[1:]
        
        if self.status not in [
            Job.Status.CANCELLED,
            Job.Status.FAILED,
            Job.Status.STASHED,
        ]:
            raise InvalidStatusException("This job cannot be submitted in its current status.")
        
        self._submit()
        
        for ancestor in ancestors:
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
        ancestors = self._ancestors(lockmode='update')[1:]
        
        if ancestors:
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
            self._status   = Job.Status.CANCELLED
            self.ts_ended = func.now()
        elif self.status == Job.Status.PROCESSING:
            self._status   = Job.Status.CANCELLING
    
    def cancel(self):
        ancestors = self._ancestors(lockmode='update')[1:]
        
        if self.status not in [
            Job.Status.FAILING,
            Job.Status.PROCESSING,
            Job.Status.QUEUED,
        ]:
            raise InvalidStatusException("This job cannot be cancelled in its current status.")
        
        self._cancel()
        
        for ancestor in ancestors:
            ancestor._update_status()
    
    def reset(self):
        ancestors = self._ancestors(lockmode='update')[1:]
        
        if ancestors:
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
    
    def __init__(self, *args, **kwargs):
        super(Job, self).__init__(*args, **kwargs)
        self._status = Job.Status.STASHED
        self._ts_created = func.now()

class Dependency(Base, model.Dependency):
    pass

class Tag(Base, model.Tag):
    # Relationships
    job = relationship('api.Job', back_populates = 'tags')
    
def init(db_url):
    session_maker = model.init(db_url)
    
    Base.metadata.create_all(bind = session_maker.bind)
    
    return session_maker