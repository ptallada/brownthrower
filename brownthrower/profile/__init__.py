#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import errno
import os
import pkg_resources
import shutil
import yaml

from brownthrower import model, release

_options = None
_profile_list = []
_default_profile = None
_current_profile = None
settings = {}

_defaults = {
    'entry_points' : {
        'task'       : 'brownthrower.task',
        'dispatcher' : 'brownthrower.dispatcher',
    },
    'paths' : {
        'profile' : os.path.expanduser(os.path.join('~', '.brownthrower'))
    },
    
    # Default settings
    'database_url' : 'sqlite:///',
}

class DoesNotExistError(Exception):
    pass

class ReservedNameError(Exception):
    pass

class InUseError(Exception):
    pass

class AlreadyExistsError(Exception):
    pass

def init_settings():
    settings.clear()
    settings.update(_defaults)
    _update_profile_list()
    profile = _parse_args()
    if profile not in ['default'] + get_available():
        create(profile)
    
    switch(profile)

def _update_profile_list():
    global _default_profile
    _profile_list[:] = os.walk(settings['paths']['profile']).next()[1]
    
    _default_profile = None
    if 'default' in _profile_list:
        _profile_list.remove('default')
        _default_profile = os.readlink(get_path('default'))

def _parse_args():
    global _options
    
    parser = argparse.ArgumentParser(prog='brownthrower')
    parser.add_argument('profile', nargs='?', default='default',
                        help="configuration profile for this session (default: '%(default)s')")
    parser.add_argument('-d', '--database-url', default=argparse.SUPPRESS,
                        help='database connection settings')
    parser.add_argument('-v', '--version', action='version', 
                        version='%%(prog)s %s' % release.__version__)
    
    _options = vars(parser.parse_args())
    
    return _options['profile']

def switch(name):
    global _current_profile
    
    if not name in ['default'] + get_available():
        raise DoesNotExistError
    
    profile = {}
    try:
        profile = yaml.safe_load(open(get_settings_path(name), 'r').read())
    except IOError as e:
        if e.errno != errno.ENOENT:
            raise
    
    settings.clear()
    settings.update(_defaults)
    if isinstance(profile, dict):
        settings.update(profile)
    settings.update(_options)
    
    if profile != {}:
        _current_profile = name
    
    if _current_profile == 'default':
        _current_profile = os.readlink(get_path('default'))
    
    from brownthrower.profile import input
    input._update_input_list()
    
    model.init(settings['database_url'])

def get_available():
    return _profile_list

def get_default():
    return _default_profile

def create(name):
    if name == 'default':
        raise ReservedNameError
    
    profile_path = get_path(name)
    if not os.path.exists(profile_path):
        os.makedirs(profile_path, 0750)
    
    settings_path = get_settings_path(name)
    if os.path.exists(settings_path):
        raise AlreadyExistsError
    else:
        settings_template = pkg_resources.resource_filename(__name__, 'settings.yaml') # @UndefinedVariable
        shutil.copyfile(settings_template, settings_path)
    
    _update_profile_list()
    
    # FIXME: Do not have any profile or input or config as default unless asked.
    # Allow having no defaults. 'reset_default' command.
    if not get_default():
        set_default(name)

def remove(name):
    if not name in get_available():
        raise DoesNotExistError
    
    if name == get_current():
        raise InUseError
    
    profile_path = get_path(name)
    shutil.rmtree(profile_path)
    
    if name == get_default():
        profile_path = get_path('default')
        os.unlink(profile_path)
    
    _update_profile_list()

def get_path(name):
    return os.path.join(settings['paths']['profile'], name)

def get_settings_path(name):
    return os.path.join(get_path(name), 'settings.yaml')

def get_current():
    return _current_profile

def set_default(name):
    global _default_profile
    
    if not name in get_available():
        raise DoesNotExistError
    
    path = get_path('default')
    try:
        os.unlink(path)
    except OSError as e:
        if e.errno != errno.ENOENT:
            raise
    os.symlink(name, path)
    _default_profile = name