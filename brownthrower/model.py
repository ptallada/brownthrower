#!/usr/bin/env python
# -*- coding: utf-8 -*-

from brownthrower.interface import constants
from sqlalchemy import event, literal_column, orm
from sqlalchemy.engine.base import Engine
from sqlalchemy.engine.url import make_url
from sqlalchemy.exc import IntegrityError, StatementError # @UnusedImport
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, scoped_session, session, joinedload # @UnusedImport
from sqlalchemy.orm.collections import attribute_mapped_collection
from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound, ObjectDeletedError # @UnusedImport
from sqlalchemy.orm.session import object_session, sessionmaker
from sqlalchemy.schema import Column, ForeignKeyConstraint, PrimaryKeyConstraint, UniqueConstraint
from sqlalchemy.sql import functions
from sqlalchemy.types import DateTime, Integer, String, Text
from sqlalchemy.engine import create_engine
import datetime

session = orm.session.Session()

Base = declarative_base()

class Job(Base):
    __tablename__ = 'job'
    __table_args__ = (
        # Primary key
        PrimaryKeyConstraint('id'),
        # Unique key
        UniqueConstraint('super_id', 'id'),
        # Foreign keys
        ForeignKeyConstraint(['super_id'], ['job.id'], onupdate='CASCADE', ondelete='RESTRICT'),
    )
    
    # Columns
    id         = Column(Integer,    nullable=False)
    super_id   = Column(Integer,    nullable=True)
    task       = Column(String(30), nullable=False)
    status     = Column(String(20), nullable=False)
    config     = Column(Text,       nullable=True)
    input      = Column(Text,       nullable=True)
    output     = Column(Text,       nullable=True)
    ts_created = Column(DateTime,   nullable=False, default=functions.now())
    ts_queued  = Column(DateTime,   nullable=True)
    ts_started = Column(DateTime,   nullable=True)
    ts_ended   = Column(DateTime,   nullable=True)
    
    # Relationships
    parents  = relationship('Job', back_populates = 'children', secondary = 'dependency',
                                   primaryjoin    = 'Dependency.child_job_id == Job.id',
                                   secondaryjoin  = 'Job.id == Dependency.parent_job_id')
    children = relationship('Job', back_populates = 'parents',  secondary = 'dependency',
                                   primaryjoin    = 'Dependency.parent_job_id == Job.id',
                                   secondaryjoin  = 'Job.id == Dependency.child_job_id')
    superjob = relationship('Job', back_populates = 'subjobs',
                                   primaryjoin    = 'Job.super_id == Job.id',
                                   remote_side    = 'Job.id')
    subjobs  = relationship('Job', back_populates = 'superjob',
                                   primaryjoin    = 'Job.super_id == Job.id',
                                   cascade        = 'all, delete-orphan', passive_deletes = True)
    tags     = relationship('Tag', back_populates = 'job', collection_class=attribute_mapped_collection('name'),
                                   cascade        = 'all, delete-orphan', passive_deletes = True)
    
    # Proxies
    tag = association_proxy('tags', 'value', creator=lambda name, value: Tag(name=name, value=value))
    
    def ancestors(self, lockmode=False):
        cls = self.__class__
        session = object_session(self)
        
        ancestors = []
        job = self
        
        if session.bind.url.drivername == 'sqlite':
            ancestors.append(job)
            while job.superjob:
                ancestors.append(job.superjob)
                job = job.superjob
            return ancestors
        
        elif session.bind.url.drivername == 'postgresql':
            l0 = literal_column('0').label('level')
            q_base = session.query(cls, l0).filter_by(
                         id = self.id
                     ).cte(recursive = True)
            l1 = literal_column('level + 1').label('level')
            q_rec = session.query(cls, l1).filter(
                        q_base.c.super_id == cls.id
                    )
            q_cte = q_base.union_all(q_rec)
            
            pending = session.query(cls).select_from(q_cte).order_by(q_cte.c.level).all()
        
        else: # Fallback for any other backend
            pending = [job]
            while job.superjob:
                pending.append(job.superjob)
                job = job.superjob
        
        session.expire(self)
        while len(pending):
            ancestors.insert(0, session.query(cls).filter_by(
                id = pending.pop().id
            ).with_lockmode(lockmode).one())
        
        return ancestors
    
    def update_status(self):
        if not self.subjobs:
            return
        
        substatus = set([subjob.status for subjob in self.subjobs])
        
        if set([constants.JobStatus.DONE]) >= substatus:
            # Need to run the epilog
            self.status = constants.JobStatus.QUEUED
        
        elif set([
            constants.JobStatus.DONE,
            constants.JobStatus.CANCELLED,
        ]) >= substatus:
            self.status = constants.JobStatus.CANCELLED
        
        elif set([
            constants.JobStatus.DONE,
            constants.JobStatus.CANCELLED,
            constants.JobStatus.FAILED,
        ]) >= substatus:
            self.status = constants.JobStatus.FAILED
        
        elif set([
            constants.JobStatus.DONE,
            constants.JobStatus.CANCELLED,
            constants.JobStatus.FAILED,
            constants.JobStatus.CANCELLING,
        ]) >= substatus:
            self.status = constants.JobStatus.CANCELLING
        
        elif set([
            constants.JobStatus.DONE,
            constants.JobStatus.CANCELLED,
            constants.JobStatus.CANCELLING,
            constants.JobStatus.PROCESSING,
            constants.JobStatus.QUEUED,
        ]) >= substatus:
            self.status = constants.JobStatus.PROCESSING
        
        else:
            self.status = constants.JobStatus.FAILING
    
    def submit(self):
        if self.subjobs:
            for subjob in self.subjobs:
                subjob.submit()
            self.update_status()
        elif self.status in [
            constants.JobStatus.STASHED,
            constants.JobStatus.FAILED,
            constants.JobStatus.CANCELLED,
        ]:
            self.status  = constants.JobStatus.QUEUED
            self.ts_queued  = datetime.datetime.now()
    
    def remove(self):
        session = object_session(self)
        if self.subjobs:
            for subjob in self.subjobs:
                subjob.remove()
            session.delete(self)
        elif self.status in [
            constants.JobStatus.STASHED,
            constants.JobStatus.FAILED,
            constants.JobStatus.CANCELLED,
        ]:
            session.delete(self)
    
    def cancel(self):
        if self.subjobs:
            for subjob in self.subjobs:
                subjob.cancel()
            self.update_status()
        elif self.status == constants.JobStatus.QUEUED:
            self.status   = constants.JobStatus.CANCELLED
            self.ts_ended = datetime.datetime.now()
        elif self.status == constants.JobStatus.PROCESSING:
            self.status   = constants.JobStatus.CANCELLING
    
    def __repr__(self):
        return u"%s(id=%s, super_id=%s, task=%s, status=%s)" % (
            self.__class__.__name__,
            repr(self.id),
            repr(self.super_id),
            repr(self.task),
            repr(self.status),
        )
    

class Dependency(Base):
    __tablename__ = 'dependency'
    __table_args__ = (
        # Primary key
        PrimaryKeyConstraint('parent_job_id', 'child_job_id'),
        # Foreign keys
        ForeignKeyConstraint(            ['parent_job_id'],                 ['job.id'], onupdate='CASCADE', ondelete='CASCADE'),
        ForeignKeyConstraint(            ['child_job_id'],                  ['job.id'], onupdate='CASCADE', ondelete='CASCADE'),
        ForeignKeyConstraint(['super_id', 'parent_job_id'], ['job.super_id', 'job.id'], onupdate='CASCADE', ondelete='CASCADE'),
        ForeignKeyConstraint(['super_id', 'child_job_id'],  ['job.super_id', 'job.id'], onupdate='CASCADE', ondelete='CASCADE'),
    )
    
    # Columns
    super_id      = Column(Integer, nullable=True)
    parent_job_id = Column(Integer, nullable=False)
    child_job_id  = Column(Integer, nullable=False)
    
    def __repr__(self):
        return u"%s(super_id=%s, parent_job_id=%s, child_job_id=%s)" % (
            self.__class__.__name__,
            repr(self.super_id),
            repr(self.parent_job_id),
            repr(self.child_job_id),
        )

class Tag(Base):
    __tablename__ = 'tag'
    __table_args__ = (
        # Primary key
        PrimaryKeyConstraint('job_id', 'name'),
        # Foreign keys
        ForeignKeyConstraint(['job_id'], ['job.id'], onupdate='CASCADE', ondelete='CASCADE'),
    )
    
    # Columns
    job_id = Column(Integer,    nullable=False)
    name   = Column(String(20), nullable=False)
    value  = Column(Text,       nullable=True)
    
    # Relationships
    job = relationship('Job', back_populates = 'tags')
    
    def __repr__(self):
        return u"%s(job_id=%s, name=%s, value=%s)" % (
            self.__class__.__name__,
            repr(self.job_id),
            repr(self.name),
            repr(self.value),
        )
        
@event.listens_for(Engine, "begin")
def _sqlite_begin(conn):
    if conn.engine.name == 'sqlite':
        # Foreign keys are NOT enabled by default... WTF!
        conn.execute("PRAGMA foreign_keys = ON")
        # Force a single active transaction on a sqlite database.
        # This is needed to emulate FOR UPDATE locks :(
        conn.execute("BEGIN EXCLUSIVE")

def init(url):
    global session
    
    url = make_url(url) 
    
    if url.drivername == 'sqlite':
        # Disable automatic transaction handling to workaround faulty nested transactions
        engine = create_engine(url, connect_args={'isolation_level':None})
    else:
        engine = create_engine(url)
    
    Base.metadata.bind = engine
    session = scoped_session(sessionmaker(bind=engine))()