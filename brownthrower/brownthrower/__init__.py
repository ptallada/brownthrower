#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

from brownthrower import model
from brownthrower.api import Job, Dependency, Tag, init

try:
    from logging import NullHandler
except ImportError:
    from logutils import NullHandler

log = logging.getLogger('brownthrower')
log.addHandler(NullHandler())
