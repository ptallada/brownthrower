#!/usr/bin/env python
# -*- coding: utf-8 -*-

import cmd
import jsonschema
import logging
import pkg_resources
import textwrap
import yaml

from brownthrower import model

# TODO: read and create a global or local configuration file
_CONFIG = {
    'entry_points.task'  : 'paudm.task',
    'entry_points.event' : 'paudm.event',
    'manager.editor'     : 'nano',
    'database.url'       : 'postgresql://tallada:secret,@db01.pau.pic.es/test_tallada',
    'listing.limit'      : 50,
}

log = logging.getLogger('paudm.manager')
# TODO: Remove
logging.basicConfig(level=logging.DEBUG)


class Manager(cmd.Cmd):
    
    def __init__(self, *args, **kwargs):
        cmd.Cmd.__init__(self, *args, **kwargs)
        
        self._tasks   = {}
        self._subcmds = {}
        
        self.intro = "\nPAU DM Manager v0.1 is ready"
    
    def preloop(self):
        self._load_tasks()
        
        from commands.job import Job
        from commands.task import Task
        
        
        self._subcmds['job'] = Job(    tasks = self._tasks,
                                      editor = _CONFIG['manager.editor'],
                                       limit = _CONFIG['listing.limit'])
        self._subcmds['task'] = Task(  tasks = self._tasks)
        
    def _load_tasks(self):
        """
        Build a list with all the Tasks available in the current environment.
        """
        
        loaded  = 0
        skipped = 0
        print "Loading available Tasks..."
        for entry in pkg_resources.iter_entry_points(_CONFIG['entry_points.task']):
            try:
                task = entry.load()
                task.check_config(yaml.load(task.get_config_template()))
                
                if entry.name in self._tasks:
                    log.warning("Cannot use Task '%s:%s'. A Task with the same name is already defined. Task is skipped." % (entry.name, entry.module_name))
                    skipped += 1
                    continue
                
                self._tasks[entry.name] = task
                loaded += 1
            
            except ImportError:
                log.warning("Could not import Task '%s:%s'" % (entry.name, entry.module_name))
                skipped += 1
            
            except jsonschema.ValidationError:
                log.warning("Arguments from Task '%s:%s' are not properly documented. Task is skipped." % (entry.name, entry.module_name))
                skipped += 1
        
        print "%d Tasks have been successfully loaded" % loaded
        print "%d Tasks have been skipped" % skipped
        
        if loaded == 0:
            print "WARNING: Could not load any Task. You will not be able to create any Job."
    
    def do_help(self, line):
        items = line.strip().split()
        if items:
            subcmd = self._subcmds.get(items[0])
            if subcmd:
                return subcmd._help(items[1:])
        
        print textwrap.dedent("""
        Documented commands (type help <topic>):
        ========================================""")
        for subcmd in self._subcmds:
            print subcmd,
        print
        print
    
    def completedefault(self, text, line, begidx, endidx):
        items = line[:begidx].strip().split()
        if len(items) > 0:
            subcmd = self._subcmds.get(items[0])
            if subcmd:
                return subcmd._complete(text, items[1:])
    
    def do_job(self, line):
        items = line.strip().split()
        self._subcmds['job']._do(items)
    
    def do_task(self, line):
        items = line.strip().split()
        self._subcmds['task']._do(items)
    
    def do_quit(self, line):
        import pdb
        pdb.set_trace()
        
        self.do_EOF(line)
    
    def do_EOF(self, line):
        return True
    
    def postloop(self):
        print

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger('sqlalchemy.engine').setLevel(logging.DEBUG)

    
    from pysrc import pydevd
    pydevd.settrace(suspend=False)
    
    #import rpdb
    #rpdb.Rpdb().set_trace()
    
    model.init(_CONFIG['database.url'])
    #model.init('sqlite:////tmp/manager.db')
    #model.Base.metadata.create_all()
    
    manager = Manager()
    manager.cmdloop()

