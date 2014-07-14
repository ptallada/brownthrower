#!/usr/bin/env python
# -*- coding: utf-8 -*-

import contextlib
import logging

from functools import wraps
from sqlalchemy import event
from sqlalchemy.engine import create_engine as sa_create_engine
from sqlalchemy.engine.url import make_url
from sqlalchemy.exc import DBAPIError

from sqlalchemy.ext.declarative import declarative_base

log = logging.getLogger('brownthrower.model')

Base = declarative_base()

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
    
    # FIXME: Do not create anything here. Move Base to bt.__init__. Write install script
    Base.metadata.create_all(bind = engine)
    
    return engine

def is_serializable_error(exc):
    if hasattr(exc.orig, 'pgcode'): 
        return exc.orig.pgcode == '40001'
    else:
        return False

def retry_on_serializable_error(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        while True:
            try:
                value = fn(*args, **kwargs)
                return value
            except DBAPIError as e:
                if not is_serializable_error(e):
                    raise
    return wrapper

# https://gist.github.com/obeattie/210032
@contextlib.contextmanager
def transactional_session(session_cls, **kwargs):
    """\
    Context manager which provides transaction management for the nested block.
    A transaction is started when the block is entered, and then either
    committed if the block exits without incident, or rolled back if an error is
    raised.
    """
    session = session_cls(**kwargs)
    try:
        yield session
        session.commit()
    except:
        # Roll back if the nested block raised an error
        session.rollback()
        raise
    finally:
        session.close()
