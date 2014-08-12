#!/usr/bin/env python
# -*- coding: utf-8 -*-

import textwrap

from . import job, task
from .base import Command
#from brownthrower.api.profile import settings

class Job(Command):
    """\
    usage: job <command> [options]
    
    Create, configure, submit and remove jobs
    """
    
    def __init__(self, *args, **kwargs):
        super(Job, self).__init__(*args, **kwargs)
        
        self.add_subcmd('cancel', job.JobCancel())
        self.add_subcmd('clone',  job.JobClone())
        self.add_subcmd('create', job.JobCreate())
        self.add_subcmd('edit',   job.JobEdit())
        self.add_subcmd('graph',  job.JobGraph())
        self.add_subcmd('link',   job.JobLink())
        self.add_subcmd('list',   job.JobList())
        self.add_subcmd('remove', job.JobRemove())
        self.add_subcmd('reset',  job.JobReset())
        self.add_subcmd('show',   job.JobShow())
        self.add_subcmd('submit', job.JobSubmit())
        self.add_subcmd('tags',   job.JobTags())
        self.add_subcmd('unlink', job.JobUnlink())

class Task(Command):
    """\
    usage: task <command> [options]
     
    Display and configure available tasks
    """
     
    def __init__(self, *args, **kwargs):
        super(Task, self).__init__(*args, **kwargs)

#         self.add_subcmd('config', task.TaskConfig())
#         self.add_subcmd('input',  task.TaskInput())
        self.add_subcmd('list',   task.TaskList())
#         self.add_subcmd('output', task.TaskOutput())
        self.add_subcmd('show',   task.TaskShow())

# class Profile(Command):
#     """\
#     usage: profile <command> [options]
#     
#     Create, edit and remove configuration profiles
#     """
#     
#     def __init__(self, *args, **kwargs):
#         super(Profile, self).__init__(*args, **kwargs)
#         
#         self.add_subcmd('create',  profile.ProfileCreate())
#         self.add_subcmd('default', profile.ProfileDefault())
#         self.add_subcmd('edit',    profile.ProfileEdit())
#         self.add_subcmd('list',    profile.ProfileList())
#         self.add_subcmd('remove',  profile.ProfileRemove())
#         self.add_subcmd('show',    profile.ProfileShow())
#         self.add_subcmd('switch',  profile.ProfileSwitch())
