#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

from . import release
from .engine import Notifications
from .model import (InvalidStatusException, TaskNotAvailableException, TokenMismatchException,
                    tasks, Dependency, Job, Tag)
from .session import (is_serializable_error, retry_on_serializable_error,
                      session_maker, transactional_session)
from .task import Task

try:
    from logging import NullHandler
except ImportError:
    from logutils import NullHandler

log = logging.getLogger('brownthrower')
log.addHandler(NullHandler())

def _setup_logging(verbosity):
    if verbosity == 0:
        logging.basicConfig(level=logging.WARNING)
        logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)
    elif verbosity == 1:
        logging.basicConfig(level=logging.INFO)
        logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)
    elif verbosity == 2:
        logging.basicConfig(level=logging.DEBUG)
        logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
