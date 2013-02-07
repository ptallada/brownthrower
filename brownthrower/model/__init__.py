#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sqlalchemy

from sqlalchemy import event
from sqlalchemy.engine.url import make_url
from sqlalchemy.exc import IntegrityError, StatementError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, scoped_session, joinedload, subqueryload
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
        UniqueConstraint('cluster_id', 'id'),
        # Foreign keys
        ForeignKeyConstraint(['cluster_id'], ['cluster.id'], onupdate='CASCADE', ondelete='RESTRICT'),
    )
    
    # Columns
    id         = Column(Integer,    nullable=False)
    # TODO: reenable nullable
    cluster_id = Column(Integer,    nullable=True)
    task       = Column(String(30), nullable=False)
    status     = Column(String(20), nullable=False)
    config     = Column(Text,       nullable=True)
    input      = Column(Text,       nullable=True)
    output     = Column(Text,       nullable=True)
    
    # Relationships
    cluster  = relationship('Cluster', back_populates='jobs') 
    parents  = relationship('Job',   back_populates='children', secondary='job_dependency',
                               primaryjoin   = 'JobDependency.child_job_id == Job.id',
                               secondaryjoin = 'Job.id == JobDependency.parent_job_id')
    children = relationship('Job',   back_populates='parents',  secondary='job_dependency',
                               primaryjoin   = 'JobDependency.parent_job_id == Job.id',
                               secondaryjoin = 'Job.id == JobDependency.child_job_id')
    
    def __repr__(self):
        return u"%s(id=%s, cluster_id=%s, task=%s, status=%s)" % (
            self.__class__.__name__,
            repr(self.id),
            repr(self.cluster_id),
            repr(self.task),
            repr(self.status),
        )
    
class Cluster(Base):
    __tablename__ = 'cluster'
    __table_args__ = (
        # Primary key
        PrimaryKeyConstraint('id'),
        # Foreign keys
    )
    
    # Columns
    id       = Column(Integer,    nullable=False)
    chain    = Column(String(30), nullable=False)
    status   = Column(String(20), nullable=False)
    config   = Column(Text,       nullable=True)
    input    = Column(Text,       nullable=True)
    output   = Column(Text,       nullable=True)
    
    # Relationships
    jobs     = relationship('Job', back_populates='cluster')
    parents  = relationship('Cluster',   back_populates='children', secondary='cluster_dependency',
                               primaryjoin   = 'ClusterDependency.child_cluster_id == Cluster.id',
                               secondaryjoin = 'Cluster.id == ClusterDependency.parent_cluster_id')
    children = relationship('Cluster',   back_populates='parents',  secondary='cluster_dependency',
                               primaryjoin   = 'ClusterDependency.parent_cluster_id == Cluster.id',
                               secondaryjoin = 'Cluster.id == ClusterDependency.child_cluster_id')
    
    def __repr__(self):
        return u"%s(id=%s, chain=%s, status=%s)" % (
            self.__class__.__name__,
            repr(self.id),
            repr(self.chain),
            repr(self.status),
        )

class JobDependency(Base):
    __tablename__ = 'job_dependency'
    __table_args__ = (
        # Primary key
        PrimaryKeyConstraint('parent_job_id', 'child_job_id'),
        # Foreign keys
        ForeignKeyConstraint(              ['parent_job_id'],                   ['job.id'], onupdate='CASCADE', ondelete='RESTRICT'),
        ForeignKeyConstraint(              ['child_job_id'],                    ['job.id'], onupdate='CASCADE', ondelete='RESTRICT'),
        ForeignKeyConstraint(['cluster_id', 'parent_job_id'], ['job.cluster_id', 'job.id'], onupdate='CASCADE', ondelete='RESTRICT'),
        ForeignKeyConstraint(['cluster_id', 'child_job_id'],  ['job.cluster_id', 'job.id'], onupdate='CASCADE', ondelete='RESTRICT'),
    )
    
    # Columns
    # TODO: re-enable nullable
    cluster_id    = Column(Integer, nullable=True)
    parent_job_id = Column(Integer, nullable=False)
    child_job_id  = Column(Integer, nullable=False)
    
    def __repr__(self):
        return u"%s(cluster_id=%s, parent_job_id=%s, child_job_id=%s)" % (
            self.__class__.__name__,
            repr(self.cluster_id),
            repr(self.parent_job_id),
            repr(self.child_job_id),
        )

class ClusterDependency(Base):
    __tablename__ = 'cluster_dependency'
    __table_args__ = (
        # Primary key
        PrimaryKeyConstraint('parent_cluster_id', 'child_cluster_id'),
        # Foreign keys
        ForeignKeyConstraint(['parent_cluster_id'], ['cluster.id'], onupdate='CASCADE', ondelete='RESTRICT'),
        ForeignKeyConstraint(['child_cluster_id'],  ['cluster.id'], onupdate='CASCADE', ondelete='RESTRICT'),
    )
    
    # Columns
    parent_cluster_id = Column(Integer, nullable=False)
    child_cluster_id  = Column(Integer, nullable=False)
    
    def __repr__(self):
        return u"%s(parent_cluster_id=%s, child_cluster_id=%s)" % (
            self.__class__.__name__,
            repr(self.parent_cluster_id),
            repr(self.child_cluster_id),
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
