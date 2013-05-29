#! /usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import pkg_resources
import signal
import sys

from . import dataset
from . import profile
from . import task

try:
    from logging import NullHandler
except ImportError:
    from logutils import NullHandler

log = logging.getLogger('brownthrower.api')
log.addHandler(NullHandler())

_tasks = {}
_dispatchers = {}

################################################################################
# PRIVATE                                                                      #
################################################################################

def _load_dispatchers(entry_point):
    """
    Build a list with all the Dispatchers available in the current environment.
    """
    
    _dispatchers.clear()
    
    for entry in pkg_resources.iter_entry_points(entry_point):
        try:
            dispatcher = entry.load()
            
            # Move to dispatcher api (validate_ep)
            assert len(dispatcher.get_help()[0]) > 0
            assert len(dispatcher.get_help()[1]) > 0
            assert '\n' not in dispatcher.get_help()[0]
            
            if dispatcher.__brownthrower_name__ in _dispatchers:
                log.warning("Skipping duplicate Dispatcher '%s' from '%s'." % (dispatcher.__brownthrower_name__, entry.module_name))
                continue
            
            _dispatchers[dispatcher.__brownthrower_name__] = dispatcher
        
        except Exception as e:
            try:
                raise
            except (AttributeError, AssertionError):
                log.warning("Dispatcher '%s' does not properly implement the interface." % entry.module_name)
            except ImportError:
                log.warning("Unable to load dispatcher from '%s'." % entry.module_name)
            finally:
                log.debug(e)

def _load_tasks(entry_point):
    """
    Build a list with all the tasks available in the current environment.
    """
    
    _tasks.clear()
    
    for entry in pkg_resources.iter_entry_points(entry_point):
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

def _setup_debugger(dbg):
    if dbg == 'pydevd':
        from pysrc import pydevd
        pydevd.settrace(suspend=True)
    
    elif dbg == 'ipdb':
        import ipdb
        ipdb.set_trace()
    
    elif dbg == 'rpdb':
        import rpdb
        rpdb.set_trace()
    
    else:
        import pdb
        pdb.set_trace()

def _system_exit(*args, **kwargs):
    sys.exit(1)

################################################################################
# PUBLIC                                                                       #
################################################################################

def init(options):
    """\
    Initialitze the API, building the list of available tasks and dispatchers in
    this environment.
    """
    
    if 'debug' in options:
        _setup_debugger(options['debug'])
    
    signal.signal(signal.SIGTERM, _system_exit)
    
    profile.init(options)
    
    _load_dispatchers('brownthrower.dispatcher')
    _load_tasks('brownthrower.task')

def get_tasks():
    return _tasks

def get_task(name):
    try:
        return _tasks[name]
    except KeyError:
        raise task.UnavailableException(name)
