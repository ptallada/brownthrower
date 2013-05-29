#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import os
import subprocess

from .base import Command, error, success, strong, warn
from brownthrower import api
from brownthrower.api.profile import settings
from tabulate import tabulate

try:
    from logging import NullHandler
except ImportError:
    from logutils import NullHandler # @UnusedImport

log = logging.getLogger('brownthrower.manager')
log.addHandler(NullHandler())

class ProfileCreate(Command):
    """\
    usage: profile create <name>
    
    Create a new configuration profile with the given name.
    """
    
    def do(self, items):
        if len(items) != 1:
            return self.help(items)
        
        try:
            api.profile.create(items[0])
            success("A new configuration profile with name '%s' has been created." % (items[0]))
        
        except Exception as e:
            try:
                raise
            except api.profile.ReservedNameError:
                error('You cannot use this name for a profile. Please, specify another name.')
            except api.profile.AlreadyExistsError:
                error('There is already a profile with this name. Please, specify another name.')
            finally:
                log.debug(e)

class ProfileDefault(Command):
    """\
    usage: profile default [ <name> ]
    
    Set or reset the default configuration profile.
    If no name is given, there will be no default configuration profile.
    """
    
    def complete(self, text, items):
        if not items:
            matching = [key
                        for key in api.profile.get_available()
                        if key.startswith(text)]
            
            return matching
    
    def do(self, items):
        if len(items) > 1:
            return self.help(items)
        
        try:
            profile = None
            if len(items) == 1:
                profile = items[0]
            
            api.profile.set_default(profile)
            if profile:
                success("The configuration profile '%s' is now the default profile." % items[0])
            else:
                success("No configuration profile is currently the default one.")
        
        except Exception as e:
            try:
                raise
            except api.profile.DoesNotExistError:
                error("There is no configuration profile named '%s'." % items[0])
            finally:
                log.debug(e)

class ProfileEdit(Command):
    """\
    usage: profile edit <name>
    
    Edit the configuration profile with the given name.
    """
    
    def complete(self, text, items):
        if not items:
            matching = [key
                        for key in api.profile.get_available()
                        if key.startswith(text)]
            
            return matching
    
    def do(self, items):
        if len(items) != 1:
            return self.help(items)
        
        try:
            path = api.profile.get_settings_path(items[0])
            assert os.access(path, os.W_OK)
            subprocess.check_call([settings['editor'], path])
            if items[0] == api.profile.get_current():
                api.profile.switch(items[0])
            
            success("The configuration profile has been successfully modified.")
            
        except Exception as e:
            try:
                raise
            except AssertionError, subprocess.CalledProcessError:
                error("Could not open configuration profile '%s' for editing." % items[0])
            finally:
                log.debug(e)

class ProfileList(Command):
    """\
    usage: profile list
    
    Show a list of all the configuration profiles available.
    """
    
    def do(self, items):
        if len(items) > 0:
            return self.help(items)
        
        profiles = api.profile.get_available()
        default  = api.profile.get_default()
        current  = api.profile.get_current()
        
        if len(profiles) == 0:
            warn("There are no profiles currently available.")
            return
        
        table = []
        for name in profiles:
            tag =        'C' if name == current else ''
            tag = tag + ('D' if name == default else '')
            table.append([name, tag])
        
        print tabulate(table, headers = ['name', ''])
        if profiles:
            print strong("'C' or 'D' in the second column designate the Current and Default profiles.")

class ProfileShow(Command):
    """\
    usage: profile show <name>
    
    Show the configuration profile with the given name.
    """
    
    def complete(self, text, items):
        if not items:
            matching = [key
                        for key in api.profile.get_available()
                        if key.startswith(text)]
            
            return matching
    
    def do(self, items):
        if len(items) != 1:
            return self.help(items)
        
        try:
            path = api.profile.get_settings_path(items[0])
            subprocess.check_call([settings['pager'], path])
        
        except Exception as e:
            try:
                raise
            except subprocess.CalledProcessError:
                error("Could not display configuration profile '%s'" % items[0])
            finally:
                log.debug(e)

class ProfileRemove(Command):
    """\
    usage: profile remove <name>
    
    Remove the specified configuration profile.
    """
    
    def complete(self, text, items):
        if not items:
            matching = [key
                        for key in api.profile.get_available()
                        if key.startswith(text)]
            
            return matching
    
    def do(self, items):
        if len(items) != 1:
            return self.help(items)
        
        try:
            api.profile.remove(items[0])
            success("The configuration profile '%s' has been removed." % items[0])
        
        except Exception as e:
            try:
                raise
            except api.profile.DoesNotExistError:
                error("There is no configuration profile named '%s'." % items[0])
            except api.profile.InUseError:
                error("Cannot remove current configuration profile.")
            finally:
                log.debug(e)

class ProfileSwitch(Command):
    """\
    usage: profile switch <name>
    
    Apply the configuration profile with the given name and switch to its settings.
    """
    
    def complete(self, text, items):
        if not items:
            matching = [key
                        for key in api.profile.get_available()
                        if key.startswith(text)]
            
            return matching
    
    def do(self, items):
        if len(items) != 1:
            return self.help(items)
        
        try:
            api.profile.switch(items[0])
            success("The configuration profile '%s' is now the current profile." % items[0])
        
        except Exception as e:
            try:
                raise
            except api.profile.DoesNotExistError:
                error("There is no configuration profile named '%s'." % items[0])
            finally:
                log.debug(e)
