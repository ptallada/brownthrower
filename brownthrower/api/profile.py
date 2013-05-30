#!/usr/bin/env python
# -*- coding: utf-8 -*-

import atexit
import errno
import os
import pkg_resources
import shutil
import yaml

from brownthrower import model

_options = None
_profile_list = []
_default_profile = None
_current_profile = None
input  = None # @ReservedAssignment
config = None

settings  = {}

_defaults = {
    'paths' : {
        'profile' : os.path.expanduser(os.path.join('~', '.brownthrower'))
    },
    
    ### Default settings
    # Database connection settings
    'database_url' : 'sqlite:///',
    
    # Command to launch a text editting application
    'editor' : 'nano',
    
    # Pager for displaying large chunks of text
    'pager' : 'less',
    
    # Max number of history lines to be preserved. -1 for no limit.
    'history_length' : 1000,
    
    # Logging configuration
    'logging' : {
        'version' : 1,
        'loggers' : {
            '' : {
                'level' : 'NOTSET',
                'handlers': [
                    'console',
                ],
            },
        },
        'handlers' : {
            'console' : {
                'class' : 'logging.StreamHandler',
                'formatter' : 'detailed',
                'stream' : 'ext://sys.stderr',
            },
        },
        'formatters' : {
            'detailed' : {
                'format' : '%(asctime)s %(name)s %(module)s line:%(lineno)d %(levelname)s %(message)s',
            },
        },
    },
}

################################################################################
# PRIVATE                                                                      #
################################################################################

def _setup_logging(config):
    try:
        from logging.config import dictConfig
    except ImportError:
        from logutils.dictconfig import dictConfig
    
    dictConfig(config)

def _setup_readline():
    try:
        import readline
        
        if get_current():
            readline.read_history_file(get_history_path(get_current()))
        
        readline.set_history_length(settings['history_length'])
        
    except ImportError:
        pass
    except IOError as e:
        if e.errno != errno.ENOENT:
            raise

def _shutdown_readline():
    try:
        import readline
        
        if get_current():
            readline.write_history_file(get_history_path(get_current()))
        
        readline.clear_history()
        
    except ImportError:
        pass
    except IOError as e:
        if e.errno != errno.ENOENT:
            raise

def _update_profile_list():
    global _default_profile
    _profile_list[:] = []
    try:
        _profile_list[:] = os.walk(settings['paths']['profile']).next()[1]
    except StopIteration:
        pass
    
    _default_profile = None
    if 'default' in _profile_list:
        _profile_list.remove('default')
        _default_profile = os.readlink(get_path('default'))

################################################################################
# PUBLIC API                                                                   #
################################################################################

class DoesNotExistError(Exception):
    pass

class ReservedNameError(Exception):
    pass

class InUseError(Exception):
    pass

class AlreadyExistsError(Exception):
    pass

def init(options):
    global input, config, _options # @ReservedAssignment
    
    _options = options
    
    settings.clear()
    settings.update(_defaults)
    _update_profile_list()
    
    from . import dataset
    
    input  = dataset.DatasetProfile('input') # @ReservedAssignment
    config = dataset.DatasetProfile('config')
    
    profile = options.get('profile', 'default')
    switch(profile)
    
    atexit.register(_shutdown_readline)

def get_available():
    return _profile_list

def get_current():
    return _current_profile

def get_default():
    return _default_profile

def get_history_path(name):
    return os.path.join(get_path(name), 'history')

def get_path(name):
    return os.path.join(settings['paths']['profile'], name)

def get_settings_path(name):
    return os.path.join(get_path(name), 'settings.yaml')

################################################################################
# PROFILE OPERATIONS                                                           #
################################################################################

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
        settings_template = pkg_resources.resource_filename('brownthrower', 'templates/settings.yaml') # @UndefinedVariable
        shutil.copyfile(settings_template, settings_path)
    
    _update_profile_list()
    
    # FIXME: Do not have any profile or input or config as default unless asked.

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

def set_default(name):
    global _default_profile
    
    if not name in [None] + get_available():
        raise DoesNotExistError
    
    path = get_path('default')
    try:
        os.unlink(path)
    except OSError as e:
        if e.errno != errno.ENOENT:
            raise
    if name:
        os.symlink(name, path)
    _default_profile = name

def switch(name):
    global _current_profile
    
    if not name in ['default'] + get_available():
        raise DoesNotExistError
    
    _shutdown_readline()
    
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
    
    input._update_dataset_list()
    config._update_dataset_list()
    
    _setup_logging(settings['logging'])
    _setup_readline()
    
    model.init(settings['database_url'])
