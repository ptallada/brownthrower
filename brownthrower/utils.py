#!/usr/bin/env python
# -*- coding: utf-8 -*-

import collections
import multiprocessing.queues
import time

from functools import wraps

def retry(tries, log):
    def retry_decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            for _ in range(tries - 1):
                try:
                    value = fn(*args, **kwargs)
                    return value
                except Exception as e:
                    log.warning("Exception «%s» caught while calling %s. Retrying..." % (e, fn))
                    time.sleep(0.5)
            
            value = fn(*args, **kwargs)
            return value
        
        return wrapper
    return retry_decorator

class SelectableQueue(multiprocessing.queues.SimpleQueue):
    def fileno(self):
        return self._reader.fileno()
    
    def poll(self, *args, **kwargs):
        return self._reader.poll(*args, **kwargs)

class InmutableSet(collections.Set):
    def __init__(self, container):
        self._container = container
    
    def __contains__(self, *args, **kwargs):
        return self._container.__contains__(*args, **kwargs)
    
    def __iter__(self, *args, **kwargs):
        return self._container.__iter__(*args, **kwargs)
    
    def __len__(self, *args, **kwargs):
        return self._container.__len__(*args, **kwargs)

class LockedContainer(object):
    def __init__(self, container):
        self._rlock = threading.RLock()
        self._container = container
    
    def __enter__(self):
        self._rlock.acquire()
        return self._container
    
    def __exit__(self, exc_type, exc_value, traceback):
        self._rlock.release()
