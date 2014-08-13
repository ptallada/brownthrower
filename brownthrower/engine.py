#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import trunk

from sqlalchemy import event
from sqlalchemy.engine import create_engine as sa_create_engine

log = logging.getLogger('brownthrower.engine')

class _QueuedTrunk(trunk.Trunk):
    def __init__(self, db_url):
        dsn = str(db_url)
        super(_QueuedTrunk, self).__init__(dsn)
     
    def fileno(self):
        return self.conn.fileno()
    
    def get(self):
        _, payload = super(_QueuedTrunk, self).get(block=False)
        return int(payload)

class Notifications(object):
    
    class Channel(object):
        CANCEL = 'bt_cancel'
        CREATE = 'bt_create'
        UPDATE = 'bt_update'
        DELETE = 'bt_delete'
    
    def __init__(self, session):
        self._session = session
        if session.bind.url.drivername != 'postgresql':
            raise NotImplementedError("LISTEN/NOTIFY only supported in PostgreSQL.")
    
    def notify(self, channel, payload):
        self._session.execute(
            "SELECT pg_notify(:channel, :payload);", {
                'channel' : channel,
                'payload' : payload,
            }
        )
    
    def job_created(self, job_id):
        self.notify(Notifications.Channel.CREATE, str(job_id))
        
    def job_updated(self, job_id):
        self.notify(Notifications.Channel.UPDATE, str(job_id))
        
    def job_deleted(self, job_id):
        self.notify(Notifications.Channel.DELETE, str(job_id))
            
    def job_cancelled(self, job_id):
        self.notify(Notifications.Channel.CANCEL, str(job_id))
    
    def listener(self, channels):
        listener = _QueuedTrunk(self._session.bind.url)
        for channel in channels:
            listener.listen(channel)
        return listener

def _sqlite_connection_begin_listener(conn):
    if conn.engine.name == 'sqlite':
        log.debug("Fixing SQLite stupid implementation.")
        # Foreign keys are NOT enabled by default... WTF!
        conn.execute("PRAGMA foreign_keys = ON")
        # Force a single active transaction on a sqlite database.
        # This is needed to emulate FOR UPDATE locks :(
        conn.execute("BEGIN EXCLUSIVE")

def create_engine(url):
    if url.drivername == 'sqlite':
        # Disable automatic transaction handling to workaround faulty nested transactions
        engine = sa_create_engine(url, connect_args={'isolation_level':None})
        event.listen(engine, 'begin', _sqlite_connection_begin_listener)
    else:
        engine = sa_create_engine(url, isolation_level="SERIALIZABLE")
    
    return engine

