#!/usr/bin/env python
# -*- coding: utf-8 -*-

import multiprocessing
import trunk

import brownthrower as bt

class SelectableQueue(multiprocessing.queues.SimpleQueue):
    def fileno(self):
        return self._reader.fileno()

class CancelQueue(trunk.Trunk):
    def __init__(self, db_url):
        dsn = str(db_url)
        super(CancelQueue, self).__init__(dsn)
        self.listen(bt.PG_CHANNEL_CANCEL)
     
    def fileno(self):
        return self.conn.fileno()
    
    def get(self):
        _, payload = super(CancelQueue, self).get(block=False)
        return int(payload)
