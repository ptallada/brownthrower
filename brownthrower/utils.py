#!/usr/bin/env python
# -*- coding: utf-8 -*-

import multiprocessing.queues

class SelectableQueue(multiprocessing.queues.SimpleQueue):
    def fileno(self):
        return self._reader.fileno()
    
    def poll(self, *args, **kwargs):
        return self._reader.poll(*args, **kwargs)