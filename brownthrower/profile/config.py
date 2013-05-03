#!/usr/bin/env python
# -*- coding: utf-8 -*-

import errno
import os
import shutil

from brownthrower import api, profile

_config_list = {}
_default_config = {}

def get_path(name):
    return os.path.join(profile.get_path(profile.get_current()), 'config', name)

def get_config_path(task, name):
    return os.path.join(get_path(task), name)

def _update_config_list():
    _config_list.clear()
    _default_config.clear()
    
    config_path = os.path.join(profile.get_path(profile.get_current()), 'config')
    names = os.walk(config_path).next()[1]
    for name in names:
        _config_list[name] = []
        _default_config[name] = None
        configs = os.walk(os.path.join(config_path, name)).next()[2]
        for config_ in configs:
            if config_ == 'default':
                _default_config[name] = os.readlink(os.path.join(config_path, name, config_))
            else:
                _config_list[name].append(config_)

def get_available(task):
    return _config_list.get(task, [])

def get_default(task):
    return _default_config.get(task, None)

def set_default(task, name):
    if not name in get_available(task):
        raise profile.DoesNotExistError
    
    path = get_config_path(task, 'default')
    try:
        os.unlink(path)
    except OSError as e:
        if e.errno != errno.ENOENT:
            raise
    os.symlink(name, path)
    _default_config[task] = name

def create(task, name):
    if name in [ 'default', 'sample' ]:
        raise profile.ReservedNameError
    
    config_path = get_path(api.get_name(task))
    if not os.path.exists(config_path):
        os.makedirs(config_path, 0750)
    
    config_path = get_config_path(api.get_name(task), name)
    if os.path.exists(config_path):
        raise profile.AlreadyExistsError
    
    # TODO: Use default if defined
    with open(config_path, 'w') as fd:
        fd.write(api.get_config_sample(task))
    
    _update_config_list()
    
    if not get_default(api.get_name(task)):
        set_default(api.get_name(task), name)

def remove(task, name):
    if not name in get_available(task):
        raise profile.DoesNotExistError
    
    path = get_config_path(task, name)
    os.unlink(path)
    
    if name == get_default(task):
        names = get_available(task)
        names.remove(name)
        if len(names) > 0:
            set_default(task, names[0])
        else:
            path = get_path(task)
            shutil.rmtree(path)
    
    _update_config_list()