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
            
            assert isinstance(task.get_config_template(), basestring)
            assert isinstance(task.get_input_template(),  basestring)
            assert isinstance(task.get_output_template(), basestring)
            
            task.check_config(task.get_config_template())
            task.check_input( task.get_input_template())
            task.check_output(task.get_output_template())
            
            if entry.name in tasks:
                log.warning("Skipping Task '%s:%s': a Task with the same name is already defined." % (entry.name, entry.module_name))
                continue
            
            tasks[entry.name] = task
        
        except (AttributeError, AssertionError):
            log.warning("Skipping Task '%s:%s': it does not properly implement the interface." % (entry.name, entry.module_name))
            continue
        
        except interface.TaskValidationException:
            log.warning("Skipping Task '%s:%s': their own templates fail to validate." % (entry.name, entry.module_name))
            continue
        
        except ImportError:
            log.warning("Skipping Task '%s:%s': unable to load." % (entry.name, entry.module_name))
    
    return tasks
