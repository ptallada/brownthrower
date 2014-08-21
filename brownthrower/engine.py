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
    
    def __iter__(self):
        return self
    
    def next(self):
        try:
            _, payload = super(_QueuedTrunk, self).get(block=False)
            return int(payload)
        except trunk.Empty:
            raise StopIteration

class _Channel(object):
    def __init__(self, session):
        self._hash = hex(hash(session.bind.url))[2:]
    
    @property
    def job_create(self):
        return 'bt_job_create_%s' % self._hash
    
    @property
    def job_update(self):
        return 'bt_job_update_%s' % self._hash
    
    @property
    def job_delete(self):
        return 'bt_job_delete_%s' % self._hash
    
    @property
    def dependency_create(self):
        return 'bt_dependency_create_%s' % self._hash
    
    @property
    def dependency_update(self):
        return 'bt_dependency_update_%s' % self._hash
    
    @property
    def dependency_delete(self):
        return 'bt_dependency_delete_%s' % self._hash
    
    @property
    def tag_create(self):
        return 'bt_tag_create_%s' % self._hash
    
    @property
    def tag_update(self):
        return 'bt_tag_update_%s' % self._hash
    
    @property
    def tag_delete(self):
        return 'bt_tag_delete_%s' % self._hash
    
    @property
    def job_all(self):
        return set([
            self.job_create,
            self.job_update,
            self.job_delete,
        ])
    
    @property
    def dependency_all(self):
        return set([
            self.dependency_create,
            self.dependency_update,
            self.dependency_delete,
        ])
    
    @property
    def tag_all(self):
        return set([
            self.tag_create,
            self.tag_update,
            self.tag_delete,
        ])
    
    def all(self):
        return self.job_all | self.dependency_all | self.tag_all

class Notifications(object):
    def __init__(self, session):
        self._session = session
        if session.bind.url.drivername != 'postgresql':
            raise NotImplementedError("LISTEN/NOTIFY only supported in PostgreSQL.")
        self.channel = _Channel(session)
    
    def notify(self, channel, payload):
        self._session.execute(
            "SELECT pg_notify(:channel, :payload);", {
                'channel' : channel,
                'payload' : payload,
            }
        )
    
    def job_create(self, job_id):
        self.notify(self.channel.job_create, str(job_id))
        
    def job_update(self, job_id):
        self.notify(self.channel.job_update, str(job_id))
        
    def job_delete(self, job_id):
        self.notify(self.channel.job_delete, str(job_id))
    
    def dependency_create(self, parent_id, child_id):
        payload = "%d-%d" % (parent_id, child_id)
        self.notify(self.channel.dependency_create, payload)
        
    def dependency_update(self, parent_id, child_id):
        payload = "%d-%d" % (parent_id, child_id)
        self.notify(self.channel.dependency_update, payload)
        
    def dependency_delete(self, parent_id, child_id):
        payload = "%d-%d" % (parent_id, child_id)
        self.notify(self.channel.dependency_delete, payload)
    
    def tag_create(self, job_id):
        self.notify(self.channel.tag_create, str(job_id))
        
    def tag_update(self, job_id):
        self.notify(self.channel.tag_update, str(job_id))
        
    def tag_delete(self, job_id):
        self.notify(self.channel.tag_delete, str(job_id))
    
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

