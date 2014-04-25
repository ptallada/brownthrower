#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

from . import release
from .model import create_engine, is_serializable_error, retry_on_serializable_error, transactional_session
from .job import Job, InvalidStatusException, TaskNotAvailableException
from .task import Task, TaskStore

try:
    from logging import NullHandler
except ImportError:
    from logutils import NullHandler

log = logging.getLogger('brownthrower')
log.addHandler(NullHandler())

tasks = TaskStore()

# def run(self):
#     with transactional_session(self.session_maker):
#         # Check requisites for running
#         if not self._impl:
#             raise TaskNotAvailableException(self.task)
#         if self.status != Job.Status.QUEUED:
#             raise InvalidStatusException("Only jobs in QUEUED status can be executed.")
#         if any([parent.status != Job.Status.DONE for parent in self.parents]):
#             raise InvalidStatusException("This job cannot be executed because not all of its parents have finished.")
#         # Moving job into PROCESSING state
#         self._status = Job.Status.PROCESSING
#         #self._ts_started = func.now()
#         for ancestor in self._ancestors():
#             ancestor._update_status()
