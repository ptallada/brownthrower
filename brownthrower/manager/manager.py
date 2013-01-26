#!/usr/bin/env python
# -*- coding: utf-8 -*-

import cmd
import jsonschema
import logging
import pkg_resources
import sys
import textwrap
import yaml

# TODO: read and create a global or local configuration file
_CONFIG = {
    'entry_points.task'  : 'paudm.task',
    'entry_points.event' : 'paudm.event',
}

log = logging.getLogger('paudm.manager')
# TODO: Remove
log.setLevel(logging.DEBUG)

class Manager(cmd.Cmd):
    
    def __init__(self, *args, **kwargs):
        cmd.Cmd.__init__(self, *args, **kwargs)
        
        self._tasks = {}
        
        self.intro = "\nPAU DM Manager v0.1 is ready"
    
    def preloop(self):
        self._load_tasks()
    
    def _load_tasks(self):
        """
        Build a list with all the Tasks available in the current environment.
        """
        
        loaded  = 0
        skipped = 0
        print "Loading available Tasks..."
        for entry in pkg_resources.iter_entry_points('paudm.tasks'):
            try:
                task = entry.load()
                task.check_arguments(yaml.load(task.get_template()))
                
                if entry.name in self._tasks:
                    log.warning("Cannot use Task '%s:%s'. A Task with the same name is already defined. Task is skipped.", (entry.name, entry.module_name))
                    skipped += 1
                    continue
                
                self._tasks[entry.name] = task
                loaded += 1
            
            except ImportError:
                log.warning("Could not import Task '%s:%s'", (entry.name, entry.module_name))
                skipped += 1
            
            except jsonschema.ValidationError:
                log.warning("Arguments from Task '%s:%s' are not properly documented. Task is skipped.", (entry.name, entry.module_name))
                skipped += 1
        
        print "%d Tasks have been successfully loaded" % loaded
        print "%d Tasks have been skipped" % skipped
        
        if loaded == 0:
            print "WARNING: Could not load any Task. You will not be able to create any Job."
    
    def help_job(self, line=None):
        print textwrap.dedent("""\
        usage: job [command] [options]
        
        Available commands:
            list      list all available job tasks
            show      list all jobs in the database
            create    create a single job
            cancel    cancel a job
            reset     reset the status of a job
        """)
    
    def do_job(self, line):
        
        def do_list(line):
            """
            Prints a list of all the load Tasks with their additional description.
            """
            max_task_len = max([len(task) for task in self._tasks])
            for name, task in self._tasks.iteritems():
                print "{0:<{width}}    {1}".format(name, task.get_help()[0], width=max_task_len)
        
        def do_show(line):
            print "job show"
        
        def do_create(line):
            print "job create"
        
        def do_cancel(line):
            print "job cancel"
        
        def do_reset(line):
            print "job reset"
        
        def do_help(line):
            task = self._tasks.get(line)
            #if task
        
        subcmd = locals().get("do_%s" % line)
        if subcmd:
            subcmd(line)
        else:
            print "job"
    
    def complete_job(self, text, line, begidx, endidx):
        subcmds = ['list', 'show', 'create', 'cancel', 'reset', 'help']
        if not text:
            return subcmds
        else:
            return [subcmd
                    for subcmd in subcmds
                    if subcmd.startswith(text)]
    
    def do_quit(self, line):
        self.do_EOF(line)
    
    def do_EOF(self, line):
        return True
    
    def postloop(self):
        print

if __name__ == '__main__':
    #load_tasks()
    Manager().cmdloop()
