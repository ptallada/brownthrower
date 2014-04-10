#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import yaml

from sqlalchemy import event, types
from sqlalchemy.engine import create_engine as sa_create_engine
from sqlalchemy.engine.url import make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.schema import (Column, ForeignKeyConstraint, Index,
                               PrimaryKeyConstraint, UniqueConstraint)
from sqlalchemy.sql import functions
from sqlalchemy.types import DateTime, Integer, String, Text

log = logging.getLogger('brownthrower.model')

class YamlText(types.TypeDecorator):
    impl = types.Text
    
    def process_bind_param(self, value, dialect):
        if value :
            return yaml.safe_dump(value, default_flow_style=False)
    
    def process_result_value(self, value, dialect):
        if value:
            return yaml.safe_load(value)

class Job(object):
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
        # Do not let subclasses redefine the model
        {'keep_existing' : True}
    )
    
    # Columns
    _id         = Column('id',         Integer,    nullable=False)
    _super_id   = Column('super_id',   Integer,    nullable=True)
    _task       = Column('task',       String(50), nullable=False)
    _status     = Column('status',     String(20), nullable=False)
    _config     = Column('config',     YamlText,   nullable=True)
    _input      = Column('input',      YamlText,   nullable=True)
    _output     = Column('output',     YamlText,   nullable=True)
    _ts_created = Column('ts_created', DateTime,   nullable=False, default=functions.now())
    _ts_queued  = Column('ts_queued',  DateTime,   nullable=True)
    _ts_started = Column('ts_started', DateTime,   nullable=True)
    _ts_ended   = Column('ts_ended',   DateTime,   nullable=True)
    
    def __repr__(self):
        return u"%s(id=%s, super_id=%s, task=%s, status=%s)" % (
            self.__class__.__name__,
            repr(self._id),
            repr(self._super_id),
            repr(self._task),
            repr(self._status),
        )

class Dependency(object):
    __tablename__ = 'dependency'
    __table_args__ = (
        # Primary key
        PrimaryKeyConstraint('parent_job_id', 'child_job_id', name='pk_dependency'),
        # Foreign keys
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

class Tag(object):
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

def _sqlite_connection_begin_listener(conn):
    if conn.engine.name == 'sqlite':
        log.debug("Fixing SQLite stupid implementation.")
        # Foreign keys are NOT enabled by default... WTF!
        conn.execute("PRAGMA foreign_keys = ON")
        # Force a single active transaction on a sqlite database.
        # This is needed to emulate FOR UPDATE locks :(
        conn.execute("BEGIN EXCLUSIVE")

def create_engine(db_url):
    url = make_url(db_url)
     
    if url.drivername == 'sqlite':
        # Disable automatic transaction handling to workaround faulty nested transactions
        engine = sa_create_engine(url, connect_args={'isolation_level':None})
        event.listen(engine, 'begin', _sqlite_connection_begin_listener)
    else:
        engine = sa_create_engine(url, isolation_level="SERIALIZABLE")
    
    return engine

_CONCURRENT_UPDATE_ERROR = '(TransactionRollbackError) could not serialize access due to concurrent update\n'
def retry_on_serializable_error(fn):
    def wrapper(*args, **kwargs):
        while True:
            try:
                return fn(*args, **kwargs)
            except DBAPIError as e:
                if e.message == _CONCURRENT_UPDATE_ERROR:
                    continue
                else:
                    raise
    return wrapper
