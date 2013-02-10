#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sqlalchemy

from sqlalchemy import event
from sqlalchemy.engine.url import make_url
from sqlalchemy.exc import IntegrityError, StatementError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, scoped_session, joinedload, subqueryload
from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound
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
        UniqueConstraint('super_id', 'id'),
        # Foreign keys
        ForeignKeyConstraint(['super_id'], ['job.id'], onupdate='CASCADE', ondelete='CASCADE'),
    )
    
    # Columns
    id         = Column(Integer,    nullable=False)
    # TODO: reenable nullable
    super_id   = Column(Integer,    nullable=True)
    task       = Column(String(30), nullable=False)
    status     = Column(String(20), nullable=False)
    config     = Column(Text,       nullable=True)
    input      = Column(Text,       nullable=True)
    output     = Column(Text,       nullable=True)
    
    # Relationships
    parents  = relationship('Job', back_populates = 'children', secondary = 'job_dependency',
                                   primaryjoin    = 'JobDependency.child_job_id == Job.id',
                                   secondaryjoin  = 'Job.id == JobDependency.parent_job_id')
    children = relationship('Job', back_populates = 'parents',  secondary = 'job_dependency',
                                   primaryjoin    = 'JobDependency.parent_job_id == Job.id',
                                   secondaryjoin  = 'Job.id == JobDependency.child_job_id')
    superjob = relationship('Job', back_populates = 'subjobs',
                                   primaryjoin    = 'Job.super_id == Job.id',
                                   remote_side    = 'Job.id')
    subjobs  = relationship('Job', back_populates = 'superjob',
                                   primaryjoin    = 'Job.super_id == Job.id',
                                   cascade        = 'all', passive_deletes = True)
    
    def __repr__(self):
        return u"%s(id=%s, super_id=%s, task=%s, status=%s)" % (
            self.__class__.__name__,
            repr(self.id),
            repr(self.super_id),
            repr(self.task),
            repr(self.status),
        )
    

class JobDependency(Base):
    __tablename__ = 'job_dependency'
    __table_args__ = (
        # Primary key
        PrimaryKeyConstraint('parent_job_id', 'child_job_id'),
        # Foreign keys
        ForeignKeyConstraint(            ['parent_job_id'],                 ['job.id'], onupdate='CASCADE', ondelete='RESTRICT'),
        ForeignKeyConstraint(            ['child_job_id'],                  ['job.id'], onupdate='CASCADE', ondelete='RESTRICT'),
        ForeignKeyConstraint(['super_id', 'parent_job_id'], ['job.super_id', 'job.id'], onupdate='CASCADE', ondelete='RESTRICT'),
        ForeignKeyConstraint(['super_id', 'child_job_id'],  ['job.super_id', 'job.id'], onupdate='CASCADE', ondelete='RESTRICT'),
    )
    
    # Columns
    # TODO: re-enable nullable
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

def _sqlite_begin(conn):
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
    
    if engine.url.drivername == 'sqlite':
        # As we have disabled the automatic transaction management, we must explicitly begin a transaction on connection open.
        # Also, that fixes another bug, by which SELECT statements do not start a new transaction.
        event.listen(engine, 'begin', _sqlite_begin)
    
    Base.metadata.bind = engine
    session = scoped_session(sessionmaker(bind=engine))()
