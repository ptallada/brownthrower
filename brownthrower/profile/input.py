#!/usr/bin/env python
# -*- coding: utf-8 -*-

import errno
import os
import shutil

from brownthrower import api, profile

_input_list = {}
_default_input = {}

def get_path(name):
    return os.path.join(profile.get_path(profile.get_current()), 'input', name)

def get_input_path(task, name):
    return os.path.join(get_path(task), name)

def _update_input_list():
    _input_list.clear()
    _default_input.clear()
    
    input_path = os.path.join(profile.get_path(profile.get_current()), 'input')
    names = os.walk(input_path).next()[1]
    for name in names:
        _input_list[name] = []
        _default_input[name] = None
        inputs = os.walk(os.path.join(input_path, name)).next()[2]
        for input_ in inputs:
            if input_ == 'default':
                _default_input[name] = os.readlink(os.path.join(input_path, name, input_))
            else:
                _input_list[name].append(input_)

def get_available(task):
    return _input_list.get(task, [])

def get_default(task):
    return _default_input.get(task, None)

def set_default(task, name):
    if not name in get_available(task):
        raise profile.DoesNotExistError
    
    path = get_input_path(task, 'default')
    try:
        os.unlink(path)
    except OSError as e:
        if e.errno != errno.ENOENT:
            raise
    os.symlink(name, path)
    _default_input[task] = name

def create(task, name):
    if name in [ 'default', 'sample' ]:
        raise profile.ReservedNameError
    
    input_path = get_path(api.get_name(task))
    if not os.path.exists(input_path):
        os.makedirs(input_path, 0750)
    
    input_path = get_input_path(api.get_name(task), name)
    if os.path.exists(input_path):
        raise profile.AlreadyExistsError
    
    # TODO: Use default if defined
    with open(input_path, 'w') as fd:
        fd.write(api.get_input_sample(task))
    
    _update_input_list()
    
    if not get_default(api.get_name(task)):
        set_default(api.get_name(task), name)

def remove(task, name):
    if not name in get_available(task):
        raise profile.DoesNotExistError
    
    path = get_input_path(task, name)
    os.unlink(path)
    
    if name == get_default(task):
        names = get_available(task)
        names.remove(name)
        if len(names) > 0:
            set_default(task, names[0])
        else:
            path = get_path(task)
            shutil.rmtree(path)
    
    _update_input_list()