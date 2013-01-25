#!/usr/bin/env python
# -*- coding: utf-8 -*-

import jsonschema
import pkg_resources
import yaml

def main():
    print "Loading tasks"
    
    tasks = {}
    for entry in pkg_resources.iter_entry_points('paudm.tasks'):
        try:
            task = entry.load()
            task.check_arguments(yaml.load(task.get_template()))
            if entry.name in tasks:
                print "Cannot use Task '%s:%s'. A Task with the same name is already defined. Task is skipped."
            tasks[entry.name] = task
        except ImportError:
            print "Could not load Task '%s:%s'" % (entry.name, entry.module_name)
        except jsonschema.ValidationError:
            print "Arguments from Task '%s:%s' are not properly documented. Task is skipped." % (entry.name, entry.module_name)
    
    print tasks

if __name__ == '__main__':
    main()
