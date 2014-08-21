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
            notifier.job_create(obj.id)
        if isinstance(obj, model.Dependency):
            notifier.dependency_create(obj.parent_id, obj.child_id)
        if isinstance(obj, model.Tag):
            notifier.tag_create(obj.job_id)
    for obj in session.dirty:
        if isinstance(obj, model.Job):
            notifier.job_update(obj.id)
        if isinstance(obj, model.Dependency):
            notifier.dependency_update(obj.parent_id, obj.child_id)
        if isinstance(obj, model.Tag):
            notifier.tag_update(obj.job_id)
    for obj in session.deleted:
        if isinstance(obj, model.Job):
            notifier.job_delete(obj.id)
        if isinstance(obj, model.Dependency):
            notifier.dependency_delete(obj.parent_id, obj.child_id)
        if isinstance(obj, model.Tag):
            notifier.tag_delete(obj.job_id)

def session_maker(dsn):
    url = make_url(dsn)
    eng = engine.create_engine(url)
    session_maker = scoped_session(sessionmaker(eng))
     
    if url.drivername == 'postgresql':
        event.listen(session_maker, 'after_flush', _postgresql_session_after_flush)
    
    model.Base.metadata.create_all(bind=eng) # @UndefinedVariable
    
    return session_maker

def is_serializable_error(exc):
    if isinstance(exc, DBAPIError):
        if hasattr(exc.orig, 'pgcode'): 
            return exc.orig.pgcode == '40001'
    
    return False

def retry_on_serializable_error(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        while True:
            try:
                value = fn(*args, **kwargs)
                return value
            except Exception as e:
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
