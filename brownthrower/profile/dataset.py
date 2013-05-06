#!/usr/bin/env python
# -*- coding: utf-8 -*-

import errno
import os
import shutil

from brownthrower import api, profile

class NoProfileIsActive(Exception):
    pass

class DatasetProfile(object):
    
    def __init__(self, dataset, sample_fn):
        self._dataset   = dataset
        self._sample_fn = sample_fn
        
        self._dataset_list    = {}
        self._dataset_default = {}
    
    def get_path(self, name):
        if not profile.get_current():
            raise NoProfileIsActive
        
        return os.path.join(profile.get_path(profile.get_current()), self._dataset, name)
    
    def get_dataset_path(self, task, name):
        return os.path.join(self.get_path(task), name)
    
    def _update_dataset_list(self):
        self._dataset_list.clear()
        self._dataset_default.clear()
        
        if profile.get_current():
            dataset_path = os.path.join(profile.get_path(profile.get_current()), self._dataset)
            names = os.walk(dataset_path).next()[1]
            for name in names:
                self._dataset_list[name] = []
                self._dataset_default[name] = None
                datasets = os.walk(os.path.join(dataset_path, name)).next()[2]
                for dataset in datasets:
                    if dataset == 'default':
                        self._dataset_default[name] = os.readlink(os.path.join(dataset_path, name, dataset))
                    else:
                        self._dataset_list[name].append(dataset)
    
    def get_available(self, task):
        return self._dataset_list.get(task, [])
    
    def get_default(self, task):
        return self._dataset_default.get(task, None)
    
    def set_default(self, task, name):
        if not name in self.get_available(task):
            raise profile.DoesNotExistError
        
        path = self.get_dataset_path(task, 'default')
        try:
            os.unlink(path)
        except OSError as e:
            if e.errno != errno.ENOENT:
                raise
        os.symlink(name, path)
        self._dataset_default[task] = name
    
    def create(self, task, name):
        if name in [ 'default', 'sample' ]:
            raise profile.ReservedNameError
        
        dataset_path = self.get_path(task)
        if not os.path.exists(dataset_path):
            os.makedirs(dataset_path, 0750)
        
        dataset_path = self.get_dataset_path(task, name)
        if os.path.exists(dataset_path):
            raise profile.AlreadyExistsError
        
        # TODO: Use default if defined
        with open(dataset_path, 'w') as fd:
            fd.write(self._sample_fn(api.get_task(task)))
        
        self._update_dataset_list()
        
        if not self.get_default(task):
            self.set_default(task, name)
    
    def remove(self, task, name):
        if not name in self.get_available(task):
            raise profile.DoesNotExistError
        
        path = self.get_dataset_path(task, name)
        try:
            os.unlink(path)
        except OSError as e:
            if e.errno != errno.ENOENT:
                raise
        
        if name == self.get_default(task):
            names = self.get_available(task)
            names.remove(name)
            if len(names) > 0:
                self.set_default(task, names[0])
            else:
                path = self.get_path(task)
                shutil.rmtree(path)
        
        self._update_dataset_list()
