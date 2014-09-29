#!/usr/bin/env python
# -*- coding: utf-8 -*-

import collections
import multiprocessing.queues
import threading
import time
import warnings

from functools import wraps

def retry(tries, log):
    """\
    Decorator to use when calling non-reliable external code that may suffer intermitent failures.
    
    @param tries: Maximum number of times to retry the call
    @type tries: int (>0)
    @param log: If the call raises an Exception, log it using this handler
    @type log: logging handler
    """
    def retry_decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            for _ in range(tries - 1):
                try:
                    value = fn(*args, **kwargs)
                    return value
                except Exception as e:
                    log.warning("Exception caught while calling %s. Retrying in one second..." % (e, fn), exc_info=True)
                    time.sleep(1)
            
            value = fn(*args, **kwargs)
            return value
        
        return wrapper
    return retry_decorator

def deprecated(func):
    """\
    This is a decorator which can be used to mark functions
    as deprecated. It will result in a warning being emmitted
    when the function is used.
    """
    def newFunc(*args, **kwargs):
        warnings.warn(
            "Call to deprecated function %s." % func.__name__,
            category=DeprecationWarning, stacklevel=2
        )
        return func(*args, **kwargs)
    
    newFunc.__name__ = func.__name__
    newFunc.__doc__ = func.__doc__
    newFunc.__dict__.update(func.__dict__)
    
    return newFunc

class SelectableQueue(multiprocessing.queues.SimpleQueue):
    """\
    Simple subclass hack to allow 'selecting' when reading.
    """
    def fileno(self):
        return self._reader.fileno()
    
    def poll(self, *args, **kwargs):
        return self._reader.poll(*args, **kwargs)

class InmutableSet(collections.Set):
    """\
    Basic implementation of an inmutable set.
    """
    def __init__(self, container):
        self._container = container
    
    def __contains__(self, *args, **kwargs):
        return self._container.__contains__(*args, **kwargs)
    
    def __iter__(self, *args, **kwargs):
        return self._container.__iter__(*args, **kwargs)
    
    def __len__(self, *args, **kwargs):
        return self._container.__len__(*args, **kwargs)

class LockedContainer(object):
    """\
    Helper class to proxy access to a container using a Lock.
    """
    def __init__(self, container):
        self._rlock = threading.RLock()
        self._container = container
    
    def __enter__(self):
        self._rlock.acquire()
        return self._container
    
    def __exit__(self, exc_type, exc_value, traceback):
        self._rlock.release()
