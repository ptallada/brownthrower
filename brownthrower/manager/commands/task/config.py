#!/usr/bin/env python
# -*- coding: utf-8 -*-

import brownthrower.interface.task # @UnusedImport
import brownthrower.profile.config # @UnusedImport
import logging
import os
import prettytable
import shutil
import subprocess
import tempfile
import textwrap

from ..base import Command, error, success, warn, strong
from brownthrower import api, profile, interface


log = logging.getLogger('brownthrower.manager')

class TaskConfigSchema(Command):
    
    def help(self, items):
        print textwrap.dedent("""\
        usage: task config schema <task>
        
        Show the schema of the config dataset for a given task.
        """)
    
    def complete(self, text, items):
        if not items:
            matching = [key
                        for key in api.get_tasks().iterkeys()
                        if key.startswith(text)]
            
            return matching
    
    def do(self, items):
        if len(items) != 1:
            return self.help(items)
        
        # FIXME: Aquesta excepció hauria de venir de la API
        task = api.get_task(items[0])
        if not task:
            error("The task '%s' is not currently available in this environment." % items[0])
            return
        
        viewer = subprocess.Popen(['pager'], stdin=subprocess.PIPE)
        viewer.communicate(input=api.get_config_schema(task))

class TaskConfigSample(Command):
    
    def help(self, items):
        print textwrap.dedent("""\
        usage: task config sample <task>
        
        Show a sample of the config dataset for a given task.
        """)
    
    def complete(self, text, items):
        if not items:
            matching = [key
                        for key in api.get_tasks().iterkeys()
                        if key.startswith(text)]
            
            return matching
    
    def do(self, items):
        if len(items) != 1:
            return self.help(items)
        
        task = api.get_task(items[0])
        if not task:
            error("The task '%s' is not currently available in this environment." % items[0])
            return
        
        viewer = subprocess.Popen(['pager'], stdin=subprocess.PIPE)
        viewer.communicate(input=api.get_config_sample(task))

class TaskConfigShow(Command):
    
    def help(self, items):
        print textwrap.dedent("""\
        usage: task config show <task> <name>
        
        Show the config dataset with the specified name for a given task.
        """)
    
    def complete(self, text, items):
        if not items:
            matching = [key
                        for key in api.get_tasks().iterkeys()
                        if key.startswith(text)]
            return matching
        if (len(items) == 1) and (items[0] in api.get_tasks().iterkeys()):
            matching = [key
                        for key in profile.config.get_available(items[0])
                        if key.startswith(text)]
            
            return matching
    
    def do(self, items):
        if (
            (len(items) != 2) or
            (items[0] not in api.get_tasks().iterkeys()) or
            (items[1] not in profile.config.get_available(items[0]))
        ):
            return self.help(items)
        
        try:
            path = profile.config.get_config_path(items[0], items[1])
            subprocess.check_call(['pager', path])
        
        except BaseException as e:
            try:
                raise
            except subprocess.CalledProcessError:
                error("Could not display config dataset '%s'." % items[0])
            finally:
                log.debug(e)

class TaskConfigEdit(Command):
    
    def help(self, items):
        print textwrap.dedent("""\
        usage: task config edit <task> <name>
        
        Edit the config dataset with the specified name for a given task.
        """)
    
    def complete(self, text, items):
        if not items:
            matching = [key
                        for key in api.get_tasks().iterkeys()
                        if key.startswith(text)]
            return matching
        if (len(items) == 1) and (items[0] in api.get_tasks().iterkeys()):
            matching = [key
                        for key in profile.config.get_available(items[0])
                        if key.startswith(text)]
            
            return matching
    
    def do(self, items):
        if (
            (len(items) != 2) or
            (items[0] not in api.get_tasks().iterkeys()) or
            (items[1] not in profile.config.get_available(items[0]))
        ):
            return self.help(items)
        
        try:
            path = profile.config.get_config_path(items[0], items[1])
            assert os.access(path, os.W_OK)
            task = api.get_task(items[0])
            
            with tempfile.NamedTemporaryFile("w+") as fh:
                shutil.copyfileobj(open(path), fh)
                fh.flush()
                
                subprocess.check_call(['editor', fh.name])
                
                fh.seek(0)
                api.validate_config(task, fh.read())
                fh.seek(0)
                shutil.copyfileobj(fh, open(path, 'w'))
            
            success("The task config dataset has been successfully modified.")
        
        except BaseException as e:
            try:
                raise
            except EnvironmentError:
                error("Unable to open the temporary dataset buffer.")
            except interface.task.ValidationException:
                error("The new value for the config dataset is not valid.")
            finally:
                log.debug(e)

class TaskConfigRemove(Command):
    
    def help(self, items):
        print textwrap.dedent("""\
        usage: task config remove <task> <name>
        
        Delete the config dataset with the specified name for a given task.
        """)
    
    def complete(self, text, items):
        if not items:
            matching = [key
                        for key in api.get_tasks().iterkeys()
                        if key.startswith(text)]
            return matching
        if (len(items) == 1) and (items[0] in api.get_tasks().iterkeys()):
            matching = [key
                        for key in profile.config.get_available(items[0])
                        if key.startswith(text)]
            
            return matching
    
    def do(self, items):
        if (
            (len(items) != 2) or
            (items[0] not in api.get_tasks().iterkeys()) or
            (items[1] not in profile.config.get_available(items[0]))
        ):
            return self.help(items)
        
        try:
            profile.config.remove(items[0], items[1])
            success("The task config dataset has been successfully removed.")
        
        except BaseException as e:
            try:
                raise
            except profile.DoesNotExistError:
                error("There is no config dataset named '%s' for the given task '%s'." % (items[1], items[0]))
            finally:
                log.debug(e)

class TaskConfigList(Command):
    
    def help(self, items):
        print textwrap.dedent("""\
        usage: task config list <task>
        
        Show a list of all the config datasets available for the given task.
        """)
    
    def complete(self, text, items):
        if not items:
            matching = [key
                        for key in api.get_tasks().iterkeys()
                        if key.startswith(text)]
            
            return matching
    
    def do(self, items):
        if len(items) != 1:
            return self.help(items)
        
        configs  = profile.config.get_available(items[0]) # @UndefinedVariable
        default = profile.config.get_default(items[0])   # @UndefinedVariable
        
        if len(configs) == 0:
            warn("There are no config datasets currently available for this task.")
            return
        
        table = prettytable.PrettyTable(['name', ''], sortby='name')
        table.align = 'l'
        for name in configs:
            tag = 'D' if name == default else ''
            table.add_row([name, tag])
        
        print table
        if configs:
            print strong("'D' in the second column designates the Default dataset.")

# TODO: allow creation from 'default' ,'sample' and <name> datasets.
class TaskConfigCreate(Command):
    
    def help(self, items):
        print textwrap.dedent("""\
        usage: task config create <task> <name>
        
        Create a new config dataset for the given task with the specified name.
        """)
    
    def complete(self, text, items):
        if not items:
            matching = [key
                        for key in api.get_tasks().iterkeys()
                        if key.startswith(text)]
            return matching
    
    def do(self, items):
        if (
            (len(items) != 2) or
            (items[0] not in api.get_tasks().iterkeys())
        ):
            return self.help(items)
        
        try:
            profile.config.create(api.get_task(items[0]), items[1])
            success("A new config dataset with name '%s' has been created." % (items[0]))
        
        except BaseException as e:
            try:
                raise
            except profile.ReservedNameError:
                error('You cannot use this name for an config dataset. Please, specify another name.')
            except profile.AlreadyExistsError:
                error('There is already an config dataset with this name. Please, specify another name.')
            finally:
                log.debug(e)

class TaskConfigDefault(Command):
    
    def help(self, items):
        print textwrap.dedent("""\
        usage: task config default <task> <name>
        
        Set the default config dataset for a task to the one with the given name.
        """)
    
    def complete(self, text, items):
        if not items:
            matching = [key
                        for key in api.get_tasks().iterkeys()
                        if key.startswith(text)]
            return matching
        if (len(items) == 1) and (items[0] in api.get_tasks().iterkeys()):
            matching = [key
                        for key in profile.config.get_available(items[0])
                        if key.startswith(text)]
            
            return matching
    
    def do(self, items):
        if (
            (len(items) != 2) or
            (items[0] not in api.get_tasks().iterkeys()) or
            (items[1] not in profile.config.get_available(items[0]))
        ):
            return self.help(items)
        
        try:
            profile.config.set_default(items[0], items[1])
            success("The config dataset '%s' is now the default for task '%s'." % (items[1], items[0]))
        
        except BaseException as e:
            try:
                raise
            except profile.DoesNotExistError:
                error("There is no configuration profile named '%s'." % items[0])
            finally:
                log.debug(e)
