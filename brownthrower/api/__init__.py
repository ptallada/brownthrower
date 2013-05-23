#! /usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import pkg_resources

from . import dataset
from . import profile
from . import task

log = logging.getLogger('brownthrower.api')

_tasks = {}
_dispatchers = {}

def init(args = None):
    """\
    Initialitze the API, building the list of available tasks and dispatchers in
    this environment.
    """
    
    _tasks.clear()
    for entry in pkg_resources.iter_entry_points('brownthrower.task'):
        try:
            t = task._validate_ep(entry)
            
            name = task.get_name(t)
            if name in _tasks:
                log.warning("Overriding task '%s' from '%s' with '%s'." % (
                    name,
                    _tasks[name].__file__,
                    t.__file__
                ))
            _tasks[name] = t
        
        except Exception as e:
            try:
                raise
            except (AttributeError, AssertionError):
                log.warning("Task '%s' does not properly implement the interface." % entry.module_name)
            except task.ValidationException:
                log.warning("Failed to validate %s dataset from task '%s'." % (e.dataset, task.get_name(e.task)))
            except ImportError:
                log.warning("Unable to import task from '%s'." % entry.module_name)
            finally:
                log.debug(e)
    
    profile.init(args)

def get_tasks():
    return _tasks

def get_task(name):
    try:
        return _tasks[name]
    except KeyError:
        raise task.UnavailableException(name)

# FIXME: Load in init
def load_dispatchers():
    """
    Build a list with all the Dispatchers available in the current environment.
    """
    
    dispatchers = {}
    
    for entry in pkg_resources.iter_entry_points('brownthrower.dispatcher'):
        try:
            dispatcher = entry.load()
            
            assert len(dispatcher.get_help()[0]) > 0
            assert len(dispatcher.get_help()[1]) > 0
            assert '\n' not in dispatcher.get_help()[0]
            
            if dispatcher.__brownthrower_name__ in dispatchers:
                log.warning("Skipping duplicate Dispatcher '%s' from '%s'." % (dispatcher.__brownthrower_name__, entry.module_name))
                continue
            
            dispatchers[dispatcher.__brownthrower_name__] = dispatcher
        
        except Exception as e:
            try:
                raise
            except (AttributeError, AssertionError):
                log.warning("Dispatcher '%s' does not properly implement the interface." % entry.module_name)
            except ImportError:
                log.warning("Unable to load Dispatcher '%s'." % entry.module_name)
            finally:
                log.debug(e)
    
    return dispatchers
