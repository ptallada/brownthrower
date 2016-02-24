#!/usr/bin/env python
# -*- coding: utf-8 -*-

import contextlib
import logging

from functools import wraps

from sqlalchemy import event
from sqlalchemy.engine.url import make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm.session import sessionmaker

from . import engine
from . import model

log = logging.getLogger('brownthrower.model')

def _postgresql_session_after_flush(session, flush_context):
    """\
    After flush event callback to push notifications on Job changes.
    """
    notifier = engine.Notifications(session)
    for obj in session.new:
        if isinstance(obj, model.Job):
            notifier.job_create(obj.id)
        elif isinstance(obj, model.Dependency):
            notifier.dependency_create(obj.parent_id, obj.child_id)
        elif isinstance(obj, model.Tag):
            notifier.tag_create(obj.job_id)
    for obj in session.dirty:
        if isinstance(obj, model.Job):
            notifier.job_update(obj.id)
        elif isinstance(obj, model.Dependency):
            notifier.dependency_update(obj.parent_id, obj.child_id)
        elif isinstance(obj, model.Tag):
            notifier.tag_update(obj.job_id)
    for obj in session.deleted:
        if isinstance(obj, model.Job):
            notifier.job_delete(obj.id)
        elif isinstance(obj, model.Dependency):
            notifier.dependency_delete(obj.parent_id, obj.child_id)
        elif isinstance(obj, model.Tag):
            notifier.tag_delete(obj.job_id)

def session_maker(dsn, initialize_db=False):
    """\
    Return a new session maker from the provided DSN.
    """
    url = make_url(dsn)
    eng = engine.create_engine(url)
    session_maker = scoped_session(sessionmaker(eng))
     
    if url.drivername == 'postgresql':
        event.listen(session_maker, 'after_flush', _postgresql_session_after_flush)
    
    if initialize_db:
        log.info("Initializing database structure on %s" % dsn)
        model.Base.metadata.create_all(bind=eng) # @UndefinedVariable
        model.Base.metadata.create_comments(bind=eng) # @UndefinedVariable
    
    return session_maker

def is_serializable_error(exc):
    """\
    Return True if the provided exception is a PostgreSQL serialization error.
    """
    if isinstance(exc, DBAPIError):
        if hasattr(exc.orig, 'pgcode'): 
            return exc.orig.pgcode == '40001'
    
    return False

def retry_on_serializable_error(fn):
    """\
    Decorator that retries a call if it fails with a serialization error.
    """
    @wraps(fn)
    def wrapper(*args, **kwargs):
        while True:
            try:
                value = fn(*args, **kwargs)
                return value
            except DBAPIError as e:
                if not is_serializable_error(e):
                    raise
                log.debug("Retrying call to «%s» due to serialization error." % fn)
    return wrapper

# https://gist.github.com/obeattie/210032
@contextlib.contextmanager
def transactional_session(session_cls, read_only=False):
    """\
    Context manager which provides transaction management for the nested block.
    A transaction is started when the block is entered, and then either
    committed if the block exits without incident, or rolled back if an error is
    raised.
    """
    session = session_cls()
    try:
        if read_only:
            session.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY DEFERRABLE")
        yield session
    except:
        # Roll back if the nested block raised an error
        session.rollback()
        raise
    else:
        session.commit()
    finally:
        session.close()
