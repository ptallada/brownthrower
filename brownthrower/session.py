#!/usr/bin/env python
# -*- coding: utf-8 -*-

import contextlib

from functools import wraps

from sqlalchemy import event
from sqlalchemy.engine.url import make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm.session import sessionmaker

from . import engine
from . import model

def _postgresql_session_after_flush(session, flush_context):
    notifier = engine.Notifications(session)
    for obj in session.new:
        if isinstance(obj, model.Job):
            notifier.job_created(obj.id)
    for obj in session.dirty:
        if isinstance(obj, model.Job):
            notifier.job_updated(obj.id)
    for obj in session.deleted:
        if isinstance(obj, model.Job):
            notifier.job_deleted(obj.id)

def session_maker(dsn):
    url = make_url(dsn)
    eng = engine.create_engine(url)
    session_maker = scoped_session(sessionmaker(eng))
     
    if url.drivername == 'postgresql':
        event.listen(session_maker, 'after_flush', _postgresql_session_after_flush)
     
    return session_maker

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
