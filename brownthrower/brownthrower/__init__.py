#!/usr/bin/env python
# -*- coding: utf-8 -*-

import collections
import logging
import pkg_resources
import traceback

from . import model, release
from .api import Job, Dependency, Tag, InvalidStatusException, create_engine
from .interface import Task
from .model import retry_on_serializable_error, is_serializable_error

try:
    from logging import NullHandler
except ImportError:
    from logutils import NullHandler

log = logging.getLogger('brownthrower')
log.addHandler(NullHandler())

class TaskStore(collections.Mapping):
    """
    Read-only mapping interface with the available tasks. 
    """
    
    def __init__(self, *args, **kwargs):
        """
        Create the list of available on initialization.
        """
        self._tasks = dict([
            (entry.name, entry)
            for entry in pkg_resources.iter_entry_points('brownthrower.task')
        ])
        log.info("Found %d tasks in this environment." % len(self._tasks))
    
    def __getitem__(self, key):
        """
        Lazy-load and return the Job class for a specified task name.
        """
        entry = self._tasks.__getitem__(key)
        if isinstance(entry, pkg_resources.EntryPoint):
            log.info("Loading task «%s» from module «%s»" % (entry.name, entry.module_name))
            try:
                entry = entry.load()
                entry._bt_name = key
                log.debug("Task '%s' successfully initialized" % key)
            
            except Exception as e:
                try:
                    raise e
                except ImportError:
                    log.debug("Could not load task '%s' from module '%s', disabling it." % (entry.name, entry.module_name))
                    del self._tasks[key]
                    return self._tasks.__getitem__(key)
                finally:
                    ex = traceback.format_exc()
                    log.debug(ex)
        
        return entry
    
    def __iter__(self):
        return self._tasks.__iter__()
    
    def __len__(self):
        return self._tasks.__len__()

tasks = TaskStore()
