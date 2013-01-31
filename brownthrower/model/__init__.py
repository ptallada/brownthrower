#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sqlalchemy

from sqlalchemy import event
from sqlalchemy.engine.url import make_url
from sqlalchemy.exc import StatementError 
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, scoped_session, eagerload, eagerload_all
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy.schema import Column, ForeignKeyConstraint, PrimaryKeyConstraint, UniqueConstraint
from sqlalchemy.types import Integer, String, Text
from sqlalchemy.engine import create_engine

session = None

Base = declarative_base()

class Job(Base):
    __tablename__ = 'job'
    __table_args__ = (
        # Primary key
        PrimaryKeyConstraint('id'),
        # Unique key
        UniqueConstraint('event_id', 'id'),
        # Foreign keys
        ForeignKeyConstraint(['event_id'], ['event.id'], onupdate='CASCADE', ondelete='RESTRICT'),
    )
    
    # Columns
    id       = Column(Integer,    nullable=False)
    # TODO: reenable nullable
    event_id = Column(Integer,    nullable=True)
    task     = Column(String(20), nullable=False)
    status   = Column(String(20), nullable=False)
    config   = Column(Text,       nullable=True)
    input    = Column(Text,       nullable=True)
    output   = Column(Text,       nullable=True)
    
    # Relationships
    event       = relationship('Event', back_populates='jobs') 
    parent_jobs = relationship('Job',   back_populates='child_jobs',  secondary='job_dependency',
                               primaryjoin   = 'JobDependency.child_job_id == Job.id',
                               secondaryjoin = 'Job.id == JobDependency.parent_job_id')
    child_jobs  = relationship('Job',   back_populates='parent_jobs', secondary='job_dependency',
                               primaryjoin   = 'JobDependency.parent_job_id == Job.id',
                               secondaryjoin = 'Job.id == JobDependency.child_job_id')
    
    @classmethod
    def lock(cls, session, mode):
        session.execute("LOCK TABLE :table IN :mode MODE", {
            'table' : cls.__tablename__,
            'mode'  : mode
        })
    
    def __repr__(self):
        return u"%s(id=%s, event_id=%s, task=%s, status=%s)" % (
            self.__class__.__name__,
            repr(self.id),
            repr(self.event_id),
            repr(self.task),
            repr(self.status),
        )
    
class Event(Base):
    __tablename__ = 'event'
    __table_args__ = (
        # Primary key
        PrimaryKeyConstraint('id'),
        # Foreign keys
    )
    
    # Columns
    id       = Column(Integer,    nullable=False)
    name     = Column(String(20), nullable=False)
    status   = Column(String(20), nullable=False)
    config   = Column(Text,       nullable=True)
    input    = Column(Text,       nullable=True)
    output   = Column(Text,       nullable=True)
    
    # Relationships
    jobs = relationship('Job', back_populates='event')

class JobDependency(Base):
    __tablename__ = 'job_dependency'
    __table_args__ = (
        # Primary key
        PrimaryKeyConstraint('parent_job_id', 'child_job_id'),
        # Foreign keys
        ForeignKeyConstraint(['event_id', 'parent_job_id'], ['job.event_id', 'job.id'], onupdate='CASCADE', ondelete='CASCADE'),
        ForeignKeyConstraint(['event_id', 'child_job_id'],  ['job.event_id', 'job.id'], onupdate='CASCADE', ondelete='CASCADE'),
    )
    
    # Columns
    # TODO: re-enable nullable
    event_id      = Column(Integer, nullable=True)
    parent_job_id = Column(Integer, nullable=False)
    child_job_id  = Column(Integer, nullable=False)

def _sqlite_begin(conn):
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
    
    if engine.url.drivername == 'sqlite':
        # As we have disabled the automatic transaction management, we must explicitly begin a transaction on connection open.
        # Also, that fixes another bug, by which SELECT statements do not start a new transaction.
        event.listen(engine, 'begin', _sqlite_begin)
    
    Base.metadata.bind = engine
    session = scoped_session(sessionmaker(bind=engine))()
