#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import pkg_resources

from brownthrower import interface

log = logging.getLogger('brownthrower.common')

def load_tasks(entry_point):
    """
    Build a list with all the Tasks available in the current environment.
    """
    
    tasks = {}
    
    for entry in pkg_resources.iter_entry_points(entry_point):
        try:
            task = entry.load()
            
            assert isinstance(task.get_config_schema(),   basestring)
            assert isinstance(task.get_input_schema(),    basestring)
            assert isinstance(task.get_output_schema(),   basestring)
            
            assert isinstance(task.get_config_sample(), basestring)
            assert isinstance(task.get_input_sample(),  basestring)
            assert isinstance(task.get_output_sample(), basestring)
            
            task.validate_config(task.get_config_sample())
            task.validate_input( task.get_input_sample())
            task.validate_output(task.get_output_sample())
            
            assert len(task.get_help()[0]) > 0
            assert len(task.get_help()[1]) > 0
            assert '\n' not in task.get_help()[0]
            
            if entry.name in tasks:
                log.warning("Skipping Task '%s:%s': a Task with the same name is already defined." % (entry.name, entry.module_name))
                continue
            
            tasks[entry.name] = task
        
        except (AttributeError, AssertionError) as e:
            log.warning("Skipping Task '%s:%s': it does not properly implement the interface." % (entry.name, entry.module_name))
            log.debug("Task '%s:%s': %s" % (entry.name, entry.module_name, e))
        except interface.TaskValidationException as e:
            log.warning("Skipping Task '%s:%s': their own samples fail to validate." % (entry.name, entry.module_name))
            log.debug("Task '%s:%s': %s" % (entry.name, entry.module_name, e))
        except ImportError:
            log.warning("Skipping Task '%s:%s': unable to load." % (entry.name, entry.module_name))
    
    return tasks

def load_dispatchers(entry_point):
    """
    Build a list with all the Dispatchers available in the current environment.
    """
    
    dispatchers = {}
    
    for entry in pkg_resources.iter_entry_points(entry_point):
        try:
            dispatcher = entry.load()
            
            assert len(dispatcher.get_help()[0]) > 0
            assert len(dispatcher.get_help()[1]) > 0
            assert '\n' not in dispatcher.get_help()[0]
            
            if entry.name in dispatchers:
                log.warning("Skipping Dispatcher '%s:%s': a Dispatcher with the same name is already defined." % (entry.name, entry.module_name))
                continue
            
            dispatchers[entry.name] = dispatcher
        
        except (AttributeError, AssertionError) as e:
            log.warning("Skipping Dispatcher '%s:%s': it does not properly implement the interface." % (entry.name, entry.module_name))
            log.debug("Dispatcher '%s:%s': %s" % (entry.name, entry.module_name, e))
        except ImportError:
            log.warning("Skipping Dispatcher '%s:%s': unable to load." % (entry.name, entry.module_name))
    
    return dispatchers