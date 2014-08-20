#!/usr/bin/env python
# -*- coding: utf-8 -*-

import multiprocessing.queues

class SelectableQueue(multiprocessing.queues.SimpleQueue):
    def fileno(self):
        return self._reader.fileno()
