#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import trunk

from sqlalchemy import event
from sqlalchemy.engine import create_engine as sa_create_engine

log = logging.getLogger('brownthrower.engine')

class _Channel(object):
    def __init__(self, session):
        self._hash = hex(hash(session.bind.url))[2:]
        
        self._job_create        = 'bt_job_create_%s'        % self._hash
        self._job_update        = 'bt_job_update_%s'        % self._hash
        self._job_delete        = 'bt_job_delete_%s'        % self._hash
        self._dependency_create = 'bt_dependency_create_%s' % self._hash
        self._dependency_update = 'bt_dependency_update_%s' % self._hash
        self._dependency_delete = 'bt_dependency_delete_%s' % self._hash
        self._tag_create        = 'bt_tag_create_%s'        % self._hash
        self._tag_update        = 'bt_tag_update_%s'        % self._hash
        self._tag_delete        = 'bt_tag_delete_%s'        % self._hash
    
    @property
    def job_create(self):
        return self._job_create
    
    @property
    def job_update(self):
        return self._job_update
    
    @property
    def job_delete(self):
        return self._job_delete
    
    @property
    def dependency_create(self):
        return self._dependency_create
    
    @property
    def dependency_update(self):
        return self._dependency_update
    
    @property
    def dependency_delete(self):
        return self._dependency_delete
    
    @property
    def tag_create(self):
        return self._tag_create
    
    @property
    def tag_update(self):
        return self._tag_update
    
    @property
    def tag_delete(self):
        return self._tag_delete
    
    @property
    def all_job_channels(self):
        return set([
            self.job_create,
            self.job_update,
            self.job_delete,
        ])
    
    @property
    def all_dependency_channels(self):
        return set([
            self.dependency_create,
            self.dependency_update,
            self.dependency_delete,
        ])
    
    @property
    def all_tag_channels(self):
        return set([
            self.tag_create,
            self.tag_update,
            self.tag_delete,
        ])
    
    @property
    def all_channels(self):
        return set.union(
            self.all_job_channels(),
            self.all_dependency_channels(),
            self.all_tag_channels(),
        )

class Notifications(trunk.Trunk):
    def __init__(self, session):
        if session.bind.url.drivername != 'postgresql':
            raise NotImplementedError("LISTEN/NOTIFY only supported in PostgreSQL.")
        
        dsn = str(session.bind.url)
        super(Notifications, self).__init__(dsn)
        
        self.channel = _Channel(session)
        self._session = session
        self._callbacks = {}
    
    def fileno(self):
        return self.conn.fileno()
    
    def listen(self, channels):
        try:
            for channel in channels:
                super(Notifications, self).listen(channel)
        except TypeError:
            super(Notifications, self).listen(channels)
    
    def notify(self, channel, payload):
        self._session.execute(
            "SELECT pg_notify(:channel, :payload);", {
                'channel' : channel,
                'payload' : payload,
            }
        )
    
    ###########################################################################
    # Methods for SENDING notifications                                       #
    ###########################################################################
    
    def job_create(self, job_id):
        self.notify(self.channel.job_create, str(job_id))
        
    def job_update(self, job_id):
        self.notify(self.channel.job_update, str(job_id))
        
    def job_delete(self, job_id):
        self.notify(self.channel.job_delete, str(job_id))
    
    def dependency_create(self, parent_id, child_id):
        payload = "%d,%d" % (parent_id, child_id)
        self.notify(self.channel.dependency_create, payload)
        
    def dependency_update(self, parent_id, child_id):
        payload = "%d,%d" % (parent_id, child_id)
        self.notify(self.channel.dependency_update, payload)
        
    def dependency_delete(self, parent_id, child_id):
        payload = "%d,%d" % (parent_id, child_id)
        self.notify(self.channel.dependency_delete, payload)
    
    def tag_create(self, job_id):
        self.notify(self.channel.tag_create, str(job_id))
        
    def tag_update(self, job_id):
        self.notify(self.channel.tag_update, str(job_id))
        
    def tag_delete(self, job_id):
        self.notify(self.channel.tag_delete, str(job_id))
    
    ###########################################################################
    # Methods for setting CALLBACKS to notifications                          #
    ###########################################################################
    
    def _set_callback(self, channel, fn):
        self._callbacks[channel] = fn
        self.listen(channel)
    
    def on_job_create(self, fn):
        self._set_callback(self.channel.job_create, fn)
    
    def on_job_update(self, fn):
        self._set_callback(self.channel.job_update, fn)
    
    def on_job_delete(self, fn):
        self._set_callback(self.channel.job_delete, fn)
    
    def on_dependency_create(self, fn):
        wrapped_fn = lambda payload: fn(*payload.split(','))
        self._set_callback(self.channel.dependency_create, wrapped_fn)
    
    def on_dependency_update(self, fn):
        wrapped_fn = lambda payload: fn(*payload.split(','))
        self._set_callback(self.channel.dependency_update, wrapped_fn)
    
    def on_dependency_delete(self, fn):
        wrapped_fn = lambda payload: fn(*payload.split(','))
        self._set_callback(self.channel.dependency_delete, wrapped_fn)
    
    def on_tag_create(self, fn):
        self._set_callback(self.channel.tag_create, fn)
    
    def on_tag_update(self, fn):
        self._set_callback(self.channel.tag_update, fn)
    
    def on_tag_delete(self, fn):
        self._set_callback(self.channel.tag_delete, fn)
    
    def has_pending(self):
        return len(self.conn.notifies) > 0
    
    def process_one(self):
        channel, payload = self.get(block=True)
        self._callbacks[channel](payload)
    
    ###########################################################################
    # Methods for RECEIVING RAW notifications                                 #
    ###########################################################################
    
    def __iter__(self):
        return self
     
    def next(self):
        """
        Returns (channel, payload)
        """
        try:
            return self.get(block=False)
        except trunk.Empty:
            raise StopIteration

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
        engine = sa_create_engine(url, isolation_level="REPEATABLE READ")
    
    return engine
