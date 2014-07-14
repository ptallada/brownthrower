#!/usr/bin/env python
# -*- coding: utf-8 -*-

import collections
import logging
import pkg_resources
import traceback

log = logging.getLogger('brownthrower.taskstore')

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
                task = entry.load()
                if task._bt_name == None:
                    task._bt_name = key
                
                if task._bt_name != key:
                    raise RuntimeError("Task from '%s' has an inconsistent name ('%s'<>'%s')" % (entry.module, task._bt_name, key))
                
                log.debug("Task '%s' successfully initialized" % key)
                
                self._tasks[key] = task
                
                return task
            
            except Exception as e:
                try:
                    raise
                except ImportError:
                    log.warning("Could not import task '%s' from module '%s', disabling it." % (entry.name, entry.module_name))
                except SyntaxError:
                    log.warning("Syntax error in task '%s' from module '%s', disabling it." % (entry.name, entry.module_name))
                except RuntimeError:
                    log.warning(e.message)
                finally:
                    ex = traceback.format_exc()
                    log.debug(ex)
                    del self._tasks[key]
                    return self._tasks.__getitem__(key)
        else:
            return entry
    
    def __iter__(self):
        return self._tasks.__iter__()
    
    def __len__(self):
        return self._tasks.__len__()

tasks = TaskStore()