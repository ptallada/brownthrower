#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import os
import prettytable
import shutil
import subprocess
import tempfile

from ..base import Command, error, strong, success, warn
from brownthrower import api, interface, profile

log = logging.getLogger('brownthrower.manager')

class TaskDatasetAttr(Command):
    @property
    def __doc__(self):
        return """\
        usage: task {dataset} {attr} <task>
        
        Show the {attr} of the {dataset} dataset for the task with the given name.
        """.format(
            dataset = self._dataset,
            attr = self._attr
        )
    
    def __init__(self, dataset, attr, attr_fn, *args, **kwargs):
        super(TaskDatasetAttr, self).__init__(*args, **kwargs)
        self._dataset = dataset
        self._attr    = attr
        self._attr_fn = attr_fn
    
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
        viewer.communicate(input=self._attr_fn(task))

class TaskDatasetShow(Command):
    @property
    def __doc__(self):
        return """\
        usage: task {dataset} show <task> <name>
        
        Show the {dataset} dataset with the specified name for a given task.
        """.format(
            dataset = self._dataset
        )
    
    def __init__(self, dataset, profile, *args, **kwargs):
        super(TaskDatasetShow, self).__init__(*args, **kwargs)
        self._dataset = dataset
        self._profile = profile
    
    def complete(self, text, items):
        if not items:
            matching = [key
                        for key in api.get_tasks().iterkeys()
                        if key.startswith(text)]
            return matching
        if (len(items) == 1) and (items[0] in api.get_tasks().iterkeys()):
            matching = [key
                        for key in self._profile.get_available(items[0])
                        if key.startswith(text)]
            
            return matching
    
    def do(self, items):
        if (
            (len(items) != 2) or
            (items[0] not in api.get_tasks().iterkeys()) or
            (items[1] not in self._profile.get_available(items[0]))
        ):
            return self.help(items)
        
        try:
            path = self._profile.get_dataset_path(items[0], items[1])
            subprocess.check_call(['pager', path])
        
        except BaseException as e:
            try:
                raise
            except subprocess.CalledProcessError:
                error("An error occurred while trying to display this dataset.")
            finally:
                log.debug(e)

class TaskDatasetEdit(Command):
    @property
    def __doc__(self):
        return """\
        usage: task {dataset} edit <task> <name>
        
        Edit the {dataset} dataset with the specified name for a given task.
        """.format(
            dataset = self._dataset
        )
    
    def __init__(self, dataset, profile, validate_fn, *args, **kwargs):
        super(TaskDatasetEdit, self).__init__(*args, **kwargs)
        self._dataset     = dataset
        self._profile     = profile
        self._validate_fn = validate_fn
    
    def complete(self, text, items):
        if not items:
            matching = [key
                        for key in api.get_tasks().iterkeys()
                        if key.startswith(text)]
            return matching
        if (len(items) == 1) and (items[0] in api.get_tasks().iterkeys()):
            matching = [key
                        for key in self._profile.get_available(items[0])
                        if key.startswith(text)]
            
            return matching
    
    def do(self, items):
        if (
            (len(items) != 2) or
            (items[0] not in api.get_tasks().iterkeys()) or
            (items[1] not in self._profile.get_available(items[0]))
        ):
            return self.help(items)
        
        try:
            path = self._profile.get_dataset_path(items[0], items[1])
            assert os.access(path, os.W_OK)
            task = api.get_task(items[0])
            
            with tempfile.NamedTemporaryFile("w+") as fh:
                shutil.copyfileobj(open(path), fh)
                fh.flush()
                
                subprocess.check_call(['editor', fh.name])
                
                fh.seek(0)
                self._validate_fn(task, fh.read())
                fh.seek(0)
                shutil.copyfileobj(fh, open(path, 'w'))
            
            success("The dataset has been successfully modified.")
        
        except BaseException as e:
            try:
                raise
            except EnvironmentError:
                error("Unable to open the temporary dataset buffer.")
            except interface.task.ValidationException:
                error("The new value for this dataset is not valid.")
            finally:
                log.debug(e)

class TaskDatasetRemove(Command):
    @property
    def __doc__(self):
        return """\
        usage: task {dataset} remove <task> <name>
        
        Delete the {dataset} dataset with the specified name for a given task.
        """.format(
            dataset = self._dataset
        )
    
    def __init__(self, dataset, profile, *args, **kwargs):
        super(TaskDatasetRemove, self).__init__(*args, **kwargs)
        self._dataset = dataset
        self._profile = profile
    
    def complete(self, text, items):
        if not items:
            matching = [key
                        for key in api.get_tasks().iterkeys()
                        if key.startswith(text)]
            return matching
        if (len(items) == 1) and (items[0] in api.get_tasks().iterkeys()):
            matching = [key
                        for key in self._profile.get_available(items[0])
                        if key.startswith(text)]
            
            return matching
    
    def do(self, items):
        if (
            (len(items) != 2) or
            (items[0] not in api.get_tasks().iterkeys()) or
            (items[1] not in self._profile.get_available(items[0]))
        ):
            return self.help(items)
        
        try:
            self._profile.remove(items[0], items[1])
            success("The dataset has been successfully removed.")
        
        except BaseException as e:
            try:
                raise
            except profile.DoesNotExistError:
                error("There is no %s dataset named '%s' for the given task '%s'." % (self._dataset, items[1], items[0]))
            finally:
                log.debug(e)

class TaskDatasetList(Command):
    @property
    def __doc__(self):
        return """\
        usage: task {dataset} list <task>
        
        Show a list of all the {dataset} datasets available for the given task.
        """.format(
            dataset = self._dataset
        )
    
    def __init__(self, dataset, profile, *args, **kwargs):
        super(TaskDatasetList, self).__init__(*args, **kwargs)
        self._dataset = dataset
        self._profile = profile
    
    def complete(self, text, items):
        if not items:
            matching = [key
                        for key in api.get_tasks().iterkeys()
                        if key.startswith(text)]
            
            return matching
    
    def do(self, items):
        if len(items) != 1:
            return self.help(items)
        
        datasets  = self._profile.get_available(items[0]) # @UndefinedVariable
        default = self._profile.get_default(items[0])   # @UndefinedVariable
        
        if len(datasets) == 0:
            warn("There are no %s datasets currently available for this task." % self._dataset)
            return
        
        table = prettytable.PrettyTable(['name', ''], sortby='name')
        table.align = 'l'
        for name in datasets:
            tag = 'D' if name == default else ''
            table.add_row([name, tag])
        
        print table
        if datasets:
            print strong("'D' in the second column designates the Default dataset.")

# TODO: allow creation from 'default' ,'sample' and <name> datasets.
class TaskDatasetCreate(Command):
    @property
    def __doc__(self):
        return """\
        usage: task {dataset} create <task> <name>
        
        Create a new {dataset} dataset for the given task with the specified name.
        """.format(
            dataset = self._dataset
        )
    
    def __init__(self, dataset, profile, *args, **kwargs):
        super(TaskDatasetCreate, self).__init__(*args, **kwargs)
        self._dataset = dataset
        self._profile = profile
    
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
            self._profile.create(api.get_task(items[0]), items[1])
            success("A new %s dataset with name '%s' has been created." % (self._dataset, items[0]))
        
        except BaseException as e:
            try:
                raise
            except profile.ReservedNameError:
                error('You cannot use this name for a dataset. Please, specify another name.')
            except profile.AlreadyExistsError:
                error('There is already a dataset with this name. Please, specify another name.')
            finally:
                log.debug(e)

class TaskDatasetDefault(Command):
    @property
    def __doc__(self):
        return """\
        usage: task {dataset} default <task> <name>
        
        Set the specified {dataset} dataset as the default one for the given task.
        """.format(
            dataset = self._dataset
        )
    
    def __init__(self, dataset, profile, *args, **kwargs):
        super(TaskDatasetDefault, self).__init__(*args, **kwargs)
        self._dataset = dataset
        self._profile = profile
    
    def complete(self, text, items):
        if not items:
            matching = [key
                        for key in api.get_tasks().iterkeys()
                        if key.startswith(text)]
            return matching
        if (len(items) == 1) and (items[0] in api.get_tasks().iterkeys()):
            matching = [key
                        for key in self._profile.get_available(items[0])
                        if key.startswith(text)]
            
            return matching
    
    def do(self, items):
        if (
            (len(items) != 2) or
            (items[0] not in api.get_tasks().iterkeys()) or
            (items[1] not in self._profile.get_available(items[0]))
        ):
            return self.help(items)
        
        try:
            self._profile.set_default(items[0], items[1])
            success("The %s dataset '%s' is now the default for the task '%s'." % (self._dataset, items[1], items[0]))
        
        except BaseException as e:
            try:
                raise
            except profile.DoesNotExistError:
                error("There is no %s dataset named '%s' for the task '%s'." % (self._dataset, items[1], items[0]))
            finally:
                log.debug(e)
