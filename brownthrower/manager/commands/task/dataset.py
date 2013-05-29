#!/usr/bin/env python
# -*- coding: utf-8 -*-

import errno
import logging
import os
import shutil
import subprocess
import tempfile

from ..base import Command, error, strong, success, warn
from brownthrower import api
from brownthrower.api.profile import settings
from tabulate import tabulate

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
    
    def __init__(self, dataset, attr, *args, **kwargs):
        super(TaskDatasetAttr, self).__init__(*args, **kwargs)
        self._dataset = dataset
        self._attr    = attr
    
    def complete(self, text, items):
        if not items:
            matching = [key
                        for key in api.get_tasks().iterkeys()
                        if key.startswith(text)]
            
            return matching
    
    def do(self, items):
        if len(items) != 1:
            return self.help(items)
        
        try:
            task = api.get_task(items[0])
            
            viewer = subprocess.Popen([settings['pager']], stdin=subprocess.PIPE)
            viewer.communicate(input=api.task.get_dataset(self._dataset, self._attr)(task))
        
        except Exception as e:
            try:
                raise
            except api.task.UnavailableException:
                error("The task '%s' is not available in this environment." % e.task)
            finally:
                log.debug(e)

class TaskDatasetShow(Command):
    @property
    def __doc__(self):
        return """\
        usage: task {dataset} show <task> <name>
        
        Show the {dataset} dataset with the specified name for a given task.
        """.format(
            dataset = self._dataset
        )
    
    def __init__(self, dataset, *args, **kwargs):
        super(TaskDatasetShow, self).__init__(*args, **kwargs)
        self._dataset = dataset
        self._profile = getattr(api.profile, self._dataset)
    
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
        if len(items) != 2:
            return self.help(items)
        
        try:
            path = self._profile.get_dataset_path(items[0], items[1])
            assert os.access(path, os.R_OK)
            subprocess.check_call([settings['pager'], path])
        
        except Exception as e:
            try:
                raise
            except AssertionError:
                error("Cannot open this dataset for reading.")
            except api.dataset.NoProfileIsActive:
                error("No configuration profile is active at this time. Please, switch into one.")
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
    
    def __init__(self, dataset, *args, **kwargs):
        super(TaskDatasetEdit, self).__init__(*args, **kwargs)
        self._dataset   = dataset
        self._profile   = getattr(api.profile, self._dataset)
        self._validator = api.task.get_validator(dataset)
    
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
        if len(items) != 2:
            return self.help(items)
        
        try:
            path = self._profile.get_dataset_path(items[0], items[1])
            assert os.access(path, os.W_OK)
            task = api.get_task(items[0])
            
            with tempfile.NamedTemporaryFile("w+") as fh:
                shutil.copyfileobj(open(path), fh)
                fh.flush()
                
                subprocess.check_call([settings['editor'], fh.name])
                
                fh.seek(0)
                self._validator(task, fh.read())
                fh.seek(0)
                shutil.copyfileobj(fh, open(path, 'w'))
            
            success("The dataset has been successfully modified.")
        
        except Exception as e:
            try:
                raise
            except api.task.UnavailableException:
                error("The task '%s' is not available in this environment." % e.task)
            except api.dataset.NoProfileIsActive:
                error("No configuration profile is active at this time. Please, switch into one.")
            except AssertionError:
                error("Cannot open this dataset for writing.")
            except EnvironmentError:
                error("Unable to open the temporary dataset buffer.")
            except subprocess.CalledProcessError:
                error("An error occurred while trying to edit this dataset.")
            except api.task.ValidationException:
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
    
    def __init__(self, dataset, *args, **kwargs):
        super(TaskDatasetRemove, self).__init__(*args, **kwargs)
        self._dataset = dataset
        self._profile = getattr(api.profile, self._dataset)
    
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
        if len(items) != 2:
            return self.help(items)
        
        try:
            self._profile.remove(items[0], items[1])
            success("The dataset has been successfully removed.")
        
        except Exception as e:
            try:
                raise
            except api.profile.DoesNotExistError:
                error("There is no %s dataset named '%s' for the given task '%s'." % (self._dataset, items[1], items[0]))
            except api.dataset.NoProfileIsActive:
                error("No configuration profile is active at this time. Please, switch into one.")
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
    
    def __init__(self, dataset, *args, **kwargs):
        super(TaskDatasetList, self).__init__(*args, **kwargs)
        self._dataset = dataset
        self._profile = getattr(api.profile, self._dataset)
    
    def complete(self, text, items):
        if not items:
            matching = [key
                        for key in api.get_tasks().iterkeys()
                        if key.startswith(text)]
            
            return matching
    
    def do(self, items):
        if len(items) != 1:
            return self.help(items)
        
        datasets = self._profile.get_available(items[0]) # @UndefinedVariable
        default = self._profile.get_default(items[0])   # @UndefinedVariable
        
        if len(datasets) == 0:
            warn("There are no %s datasets currently available for this task." % self._dataset)
            return
        
        table = []
        for name in sorted(datasets):
            table.append([name, 'D' if name == default else ''])
        
        print tabulate(table, headers=['name', ''])
        if datasets:
            print strong("'D' in the second column designates the Default dataset.")

class TaskDatasetCreate(Command):
    @property
    def __doc__(self):
        return """\
        usage: task {dataset} create <task> <name> [ from <reference> ]
        
        Create a new {dataset} dataset for the given task with the specified name.
        Optionally, a reference can be specified to indicate the initial value of this
        new dataset. The valid values for this reference are:
          - default : the default {dataset} dataset for this task
          - sample  : the sample {dataset} dataset for this task
          - <name>  : a user defined {dataset} dataset for this task
        """.format(
            dataset = self._dataset
        )
    
    def __init__(self, dataset, *args, **kwargs):
        super(TaskDatasetCreate, self).__init__(*args, **kwargs)
        self._dataset = dataset
        self._profile = getattr(api.profile, self._dataset)
    
    def complete(self, text, items):
        if not items:
            matching = [key
                        for key in api.get_tasks().iterkeys()
                        if key.startswith(text)]
            return matching
        
        elif (
            (len(items) == 2) and
            (items[0] in api.get_tasks().iterkeys())
        ):
            return ['from']
        
        elif (
            (len(items) == 3) and 
            (items[0] in api.get_tasks().iterkeys()) and
            (items[2] == 'from')
        ):
            available = ['sample']
            if self._profile.get_default(items[0]):
                available.append('default')
            available.extend(self._profile.get_available(items[0]))
            return [
                name
                for name in sorted(available)
                if name.startswith(text)
            ]
    
    def do(self, items):
        if len(items) not in (2, 4):
            return self.help(items)
        
        try:
            task = api.get_task(items[0])
            
            contents = None
            if len(items) == 4:
                from_ = items[3]
                if from_ == 'sample':
                    contents = api.task.get_dataset(self._dataset, 'sample')(task)
                elif from_ == 'default':
                    default = self._profile.get_default(items[0])
                    path = self._profile.get_dataset_path(items[0], default)
                    contents = open(path, 'r').read()
                else:
                    path = self._profile.get_dataset_path(items[0], items[3])
                    contents = open(path, 'r').read()
            else:
                default = self._profile.get_default(items[0])
                if default:
                    default_path = self._profile.get_dataset_path(items[0], default)
                    contents = open(default_path, 'r').read()
                else:
                    contents = api.task.get_dataset(self._dataset, 'sample')(task)
            
            self._profile.create(items[0], items[1])
            
            dataset_path = self._profile.get_dataset_path(items[0], items[1])
            with open(dataset_path, 'w') as fh:
                fh.write(contents)
            
            success("A new %s dataset with name '%s' has been created." % (self._dataset, items[0]))
        
        except Exception as e:
            try:
                raise
            except IOError:
                if e.errno != errno.ENOENT:
                    raise
                error("The specified reference dataset '%s' does not exist." % items[3])
            except api.task.UnavailableException:
                error("The task '%s' is not available in this environment." % e.task)
            except api.profile.ReservedNameError:
                error('You cannot use this name for a dataset. Please, specify another name.')
            except api.profile.AlreadyExistsError:
                error('There is already a dataset with this name. Please, specify another name.')
            except api.dataset.NoProfileIsActive:
                error("No configuration profile is active at this time. Please, switch into one.")
            finally:
                log.debug(e)
                try:
                    # Delete incomplete dataset
                    self._profile.remove(items[0], items[1])
                except Exception:
                    pass

class TaskDatasetDefault(Command):
    @property
    def __doc__(self):
        return """\
        usage: task {dataset} default <task> [ <name> ]
        
        Set or reset the default {dataset} dataset for the given task.
        If no name is specified, the task is left with no default {dataset} dataset.
         
        """.format(
            dataset = self._dataset
        )
    
    def __init__(self, dataset, *args, **kwargs):
        super(TaskDatasetDefault, self).__init__(*args, **kwargs)
        self._dataset = dataset
        self._profile = getattr(api.profile, self._dataset)
    
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
        if len(items) not in [1, 2]:
            return self.help(items)
        
        try:
            dataset = None 
            if len(items) == 2:
                dataset = items[1]
            
            self._profile.set_default(items[0], dataset)
            if dataset:
                success("The %s dataset '%s' is now the default for the task '%s'." % (self._dataset, items[1], items[0]))
            else:
                success("The task '%s' now has no default %s dataset.'" % (items[0], self._dataset))
        
        except Exception as e:
            try:
                raise
            except api.profile.DoesNotExistError:
                error("There is no %s dataset named '%s' for the task '%s'." % (self._dataset, items[1], items[0]))
            except api.dataset.NoProfileIsActive:
                error("No configuration profile is active at this time. Please, switch into one.")
            finally:
                log.debug(e)
