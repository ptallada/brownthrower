#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sqlalchemy

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, scoped_session
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy.schema import Column, ForeignKeyConstraint, PrimaryKeyConstraint
from sqlalchemy.types import Integer, String, Text
from sqlalchemy.engine import create_engine

session = None

Base = declarative_base()

class Job(Base):
    __tablename__ = 'job'
    __table_args__ = (
        # Primary key
        PrimaryKeyConstraint('id'),
        # Foreign keys
        ForeignKeyConstraint(['event_id'], ['event.id'], onupdate='CASCADE', ondelete='RESTRICT'),
    )
    
    # Columns
    id       = Column(Integer,    nullable=False)
    event_id = Column(Integer,    nullable=False)
    name     = Column(String(20), nullable=False)
    status   = Column(String(20), nullable=False)
    config   = Column(Text,       nullable=True)
    result   = Column(Text,       nullable=True)
    
    # Relationships
    event       = relationship('Event', back_populates='jobs') 
    parent_jobs = relationship('Job',   back_populates='child_jobs',  secondary='job_dependency',
                               primaryjoin   = 'JobDependency.child_job_id == Job.id',
                               secondaryjoin = 'Job.id == JobDependency.parent_job_id')
    child_jobs  = relationship('Job',   back_populates='parent_jobs', secondary='job_dependency',
                               primaryjoin   = 'JobDependency.parent_job_id == Job.id',
                               secondaryjoin = 'Job.id == JobDependency.child_job_id')
    
class Event(Base):
    __tablename__ = 'event'
    __table_args__ = (
        # Primary key
        PrimaryKeyConstraint('id'),
        # Foreign keys
    )
    
    # Columns
    id     = Column(Integer,    nullable=False)
    name   = Column(String(20), nullable=False)
    status = Column(String(20), nullable=False)
    
    # Relationships
    jobs = relationship('Job', back_populates='event')

class JobDependency(Base):
    __tablename__ = 'job_dependency'
    __table_args__ = (
        # Primary key
        PrimaryKeyConstraint('parent_job_id', 'child_job_id'),
        # Foreign keys
        ForeignKeyConstraint(['event_id', 'parent_job_id'], ['job.event_id', 'job.id'], onupdate='CASCADE', ondelete='CASCADE'),
        ForeignKeyConstraint(['event_id', 'child_job_id'], ['job.event_id', 'job.id'], onupdate='CASCADE', ondelete='CASCADE'),
    )
    
    # Columns
    event_id      = Column(Integer, nullable=False)
    parent_job_id = Column(Integer, nullable=False)
    child_job_id  = Column(Integer, nullable=False)

def init(url):
    global session
    
    engine = create_engine(url)
    
    Base.metadata.bind = engine
    session = scoped_session(sessionmaker(bind=engine))()

#init('sqlite:///')
#Base.metadata.create_all()
#e1 = Event(name='e1', status='NEW')
#j1 = Job(name='j1', status='NEW', event=e1)
#session.add(e1)
#session.commit()
