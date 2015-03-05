#!/usr/bin/env python
# -*- coding: utf-8 -*-

import collections
import logging
import pkg_resources

log = logging.getLogger('brownthrower.taskstore')

class TaskStore(collections.Mapping):
    """\
    Read-only mapping interface containing all the available tasks. 
    """
    
    def __init__(self, *args, **kwargs):
        """\
        Create the list of available tasks on initialization.
        """
        self._tasks = dict([
            (entry.name, entry)
            for entry in pkg_resources.iter_entry_points('brownthrower.task')
        ])
    
    def __getitem__(self, key):
        """\
        Lazy-load and return the Job class for a specified task name.
        
        @param key: Name of the task
        @type key: string
        @return: The class that implements the requested Task.
        @rtype: Task
        """
        entry = self._tasks.__getitem__(key)
        if isinstance(entry, pkg_resources.EntryPoint):
            log.info("Loading task «%s» from module «%s»" % (entry.name, entry.module_name))
            try:
                task = entry.load()
                if task._bt_name == None:
                    task._bt_name = key
                
                if task._bt_name != key:
                    raise RuntimeError("Task from '%s' has an inconsistent name ('%s'!='%s')" % (entry.module_name, task._bt_name, key))
                
                self._tasks[key] = task
                
                return task
            
            except (ImportError, SyntaxError, RuntimeError) as e:
                try:
                    raise
                except ImportError:
                    log.error("Could not import task '%s' from module '%s'" % (entry.name, entry.module_name), exc_info=True)
                except SyntaxError:
                    log.error("Syntax error in task '%s' from module '%s'" % (entry.name, entry.module_name), exc_info=True)
                except RuntimeError:
                    log.error(e.message)
                finally:
                    log.info("Disabling task '%s' from module '%s'" % (entry.name, entry.module_name))
                    del self._tasks[key]
                    return self._tasks.__getitem__(key)
        else:
            return entry
    
    def __iter__(self):
        return self._tasks.__iter__()
    
    def __len__(self):
        return self._tasks.__len__()
