#! /usr/bin/env python
# -*- coding: utf-8 -*-

import contextlib
import errno
import glite.ce.job
import logging
import select
import string
import tempfile
import threading
import time
import uuid

from sqlalchemy.orm.exc import NoResultFound

import brownthrower as bt
import brownthrower.utils as utils

log = logging.getLogger('brownthrower.dispatcher.static.threads')

# FIXME: pkg_resources i centralitzat
JDL_TEMPLATE = """\
[
Type = "Job";
JobType = "Normal";
Executable = "${executable}";
Arguments = "${arguments}";
StdOutput = "std.out";
StdError = "std.err";
OutputSandbox = {"std.out", "std.err"}; 
OutputSandboxBaseDestUri="gsiftp://localhost";
requirements = other.GlueCEStateStatus == "Production";
rank = -other.GlueCEStateEstimatedResponseTime;
RetryCount = 0;
]
"""

class LauncherThread(threading.Thread):
    """
    This thread launches a pilot for each runnable job and stores the new status
    info of gLite jobs.
    
    A dummy element is pushed to 'refresh' queue to signal the main thread
    to update the UI.
    """
    
    POLL_INTERVAL = 30
    
    def __init__(self, session_maker, allowed_tasks, runner_path, runner_args, ce_queue,
                 bt_status, bt_ids, glite_status, glite_ids, pool_size, refresh):
        super(LauncherThread, self).__init__(name='job_launcher')
        self._session_maker = session_maker
        self._allowed_tasks = allowed_tasks
        self._runner_path = runner_path
        self._runner_args = runner_args
        self._ce_queue = ce_queue
        self._bt_status = bt_status
        self._bt_ids = bt_ids
        self._glite_status = glite_status
        self._glite_ids = glite_ids
        self._refresh = refresh
        self._pool_size = pool_size
        self._token = uuid.uuid1().hex
        self._q_stop = utils.SelectableQueue()
    
    def stop(self):
        self._q_stop.put(True)
    
    @contextlib.contextmanager
    def _write_jdl(self):
        template = string.Template(JDL_TEMPLATE)
        
        with tempfile.NamedTemporaryFile("w+") as fh:
            fh.write(template.substitute({
                'executable' : self._runner_path,
                'arguments' : "%s --loop" % self._runner_args,
            }))
            
            fh.flush()
            
            yield fh.name
    
    def _actual_pool_size(self):
        with self._glite_status as glite_status:
            actual_size = 0
            for status in glite.ce.job.Status.processing:
                actual_size += glite_status[status]
            
            return actual_size
    
    @utils.retry(tries=3, log=log)
    def _launch_pilot(self):
        with self._write_jdl() as jdl_file:
            return glite.ce.job.submit(jdl_file, endpoint=self._ce_queue)
    
    def _launch_one(self):
        with self._glite_ids as glite_ids:
            with self._glite_status as glite_status:
                pilot_id = self._launch_pilot()
                
                glite_ids[pilot_id] = glite.ce.job.Status.REGISTERED
                glite_status[glite.ce.job.Status.REGISTERED] += 1
                
                return True
    
    def _launch_pending(self):
        actual_size = self._actual_pool_size()
        
        while actual_size < self._pool_size:
            self._launch_one()
            actual_size += 1
            
            self._refresh.put(True)
            
            if self._q_stop.poll():
                break
    
    def _cancel_pool(self):
        with self._glite_ids as glite_ids:
            ids = glite_ids.copy()
        
        for pilot_id, status in ids.iteritems():
            if status not in glite.ce.job.Status.final:
                try:
                    glite.ce.job.cancel(pilot_id)
                except RuntimeError:
                    pass
            
            if self._q_stop.poll():
                break
    
    def run(self):
        self._launch_pending()
        
        while not self._q_stop.poll():
            try:
                r, _, _ = select.select([self._q_stop], [], [], LauncherThread.POLL_INTERVAL)
                
                if self._q_stop in r:
                    continue
                
                self._launch_pending()
            
            except select.error as e:
                if e.args[0] != errno.EINTR:
                    raise
        
        self._q_stop.get()
        
        # Cancelling pool
        self._cancel_pool()
        
        while not self._q_stop.poll():
            try:
                r, _, _ = select.select([self._q_stop], [], [], LauncherThread.POLL_INTERVAL)
                
                if self._q_stop in r:
                    continue
                
                self._cancel_pool()
            
            except select.error as e:
                if e.args[0] != errno.EINTR:
                    raise
        
        self._q_stop.get()

class BtMonitorThread(threading.Thread):
    """
    This thread uses the BT notifications mechanism to update the status of BT
    jobs.
    
    A dummy element is pushed to 'refresh' queue to signal the main thread to
    update the UI.
    """
    
    def __init__(self, session_maker, bt_ids, bt_status, allowed_tasks, refresh):
        super(BtMonitorThread, self).__init__(name='bt_monitor')
        self._session_maker = session_maker
        self._bt_ids = bt_ids
        self._bt_status = bt_status
        self._allowed_tasks = allowed_tasks
        self._refresh = refresh
        self._q_stop = utils.SelectableQueue()
        
        self._q_changes = bt.Notifications(self._session_maker)
        self._q_changes.listen(self._q_changes.channel.all_job_channels)
        self._q_changes.listen(self._q_changes.channel.all_dependency_channels)
    
    def stop(self):
        self._q_stop.put(True)
    
    def _update_runnable(self):
        with bt.transactional_session(self._session_maker) as session:
            count = session.query(bt.Job).filter(
                bt.Job.status == bt.Job.Status.QUEUED,
                bt.Job.name.in_(self._allowed_tasks), # @UndefinedVariable
                ~ bt.Job.parents.any(bt.Job.status != bt.Job.Status.DONE) # @UndefinedVariable
            ).count()
            
            with self._bt_status as bt_status:
                bt_status['RUNNABLE'] = count
        
    
    def _initial_status(self):
        with bt.transactional_session(self._session_maker) as session:
            jobs = session.query(bt.Job).filter(
                bt.Job.name.in_(self._allowed_tasks), # @UndefinedVariable
            )
            
            with self._bt_ids as bt_ids:
                with self._bt_status as bt_status:
                    for job in jobs:
                        bt_ids[job.id] = job.status
                        bt_status[job.status] += 1
    
    def _update_job(self, job_id):
        try:
            with bt.transactional_session(self._session_maker) as session:
                job = session.query(bt.Job).filter(bt.Job.id == job_id).one()
                
                with self._bt_ids as bt_ids:
                    with self._bt_status as bt_status:
                        if job.id in bt_ids:
                            old_status = bt_ids[job.id]
                            bt_status[old_status] -= 1
                        bt_status[job.status] += 1
                        bt_ids[job.id] = job.status
                
                return job.status
        
        except NoResultFound:
            with self._bt_ids as bt_ids:
                with self._bt_status as bt_status:
                    if job_id in bt_ids:
                        old_status = bt_ids[job_id]
                        del bt_ids[job_id]
                        bt_status[old_status] -= 1
    
    def run(self):
        self._update_runnable()
        self._initial_status()
        
        while not self._q_stop.poll():
            try:
                r, _, _ = select.select([self._q_changes, self._q_stop], [], [])
                
                if self._q_stop in r:
                    continue
                
                if self._q_changes in r:
                    for channel, payload in self._q_changes:
                        if channel in self._q_changes.channel.all_job_channels:
                            status = self._update_job(int(payload))
                        
                        if self._q_stop.poll():
                            break
                    
                    if self._q_stop.poll():
                        break
                    
                    self._update_runnable()
                    
                    self._refresh.put(True)
            
            except select.error as e:
                if e.args[0] != errno.EINTR:
                    raise
        
        self._q_stop.get()

class GliteMonitorThread(threading.Thread):
    """
    This thread uses the 'glite.ce.event-query' command to update the status of
    gLite jobs.
    
    A dummy element is pushed to 'refresh' queue to signal the main thread to
    update the UI.
    """
    
    POLL_INTERVAL = 5
    
    def __init__(self, ce_queue, glite_ids, glite_status, refresh):
        super(GliteMonitorThread, self).__init__(name='glite_monitor')
        self._ce_host = ce_queue.split('/')[0]
        self._glite_ids = glite_ids
        self._glite_status = glite_status
        self._refresh = refresh
        self._q_stop = utils.SelectableQueue()
        self._last_event = 0
        
        self._init_status()
    
    def stop(self):
        self._q_stop.put(True)
    
    @utils.retry(tries=3, log=log)
    def _init_status(self):
        self._last_event = glite.ce.last_event_id(self._ce_host)
    
    @utils.retry(tries=3, log=log)
    def _pending_events(self):
        return glite.ce.event_query(self._ce_host, self._last_event)
    
    def _update_status(self):
        event = None
        for event in self._pending_events():
            with self._glite_ids as glite_ids:
                with self._glite_status as glite_status:
                    if event['jobId'] in glite_ids:
                        old_status = glite_ids[event['jobId']]
                        glite_status[old_status] -= 1
                        glite_status[event['status']] += 1
                        glite_ids[event['jobId']] = event['status']
            
            self._last_event = int(event['EventID'])
            
            if self._q_stop.poll():
                break
        
        if event:
            self._refresh.put(True)
    
    def _actual_pool_size(self):
        with self._glite_status as glite_status:
            actual_size = 0
            for status in glite.ce.job.Status.processing:
                actual_size += glite_status[status]
            
            return actual_size
    
    def run(self):
        self._update_status()
        
        while not self._q_stop.poll():
            try:
                r, _, _ = select.select([self._q_stop], [], [], GliteMonitorThread.POLL_INTERVAL)
                
                self._update_status()
                
                if self._q_stop in r:
                    continue
            
            except select.error as e:
                if e.args[0] != errno.EINTR:
                    raise
        
        self._q_stop.get()
        
        # Wait until the pool is empty
        while self._actual_pool_size():
            time.sleep(GliteMonitorThread.POLL_INTERVAL)
            self._update_status()
