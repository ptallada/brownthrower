#! /usr/bin/env python
# -*- coding: utf-8 -*-

import contextlib
import errno
import glite.ce.job
import logging
import select
import string
import sys
import tempfile
import threading
import traceback
import time
import uuid

from functools import wraps
from sqlalchemy.orm.exc import NoResultFound

import brownthrower as bt
import brownthrower.utils as utils

log = logging.getLogger('brownthrower.dispatcher.on_demand.threads')

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

def retry(tries):
    def retry_decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            for _ in range(tries - 1):
                try:
                    value = fn(*args, **kwargs)
                    return value
                except Exception as e:
                    log.warning("Exception «%s» caught while calling %s. Retrying..." % (e, fn))
                    time.sleep(0.5)
            
            value = fn(*args, **kwargs)
            return value
        
        return wrapper
    return retry_decorator

class LauncherThread(threading.Thread):
    """
    This thread launches a pilot for each runnable job and stores the new status
    info of gLite jobs.
    
    A dummy element is pushed to 'refresh' queue to signal the main thread
    to update the UI.
    """
    
    POLL_INTERVAL = 30
    
    def __init__(self, session_maker, allowed_tasks, runner_path, runner_args, ce_queue,
                 bt_status, bt_ids, glite_status, glite_ids, refresh):
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
        self._token = uuid.uuid1().hex
        self._q_stop = utils.SelectableQueue()
    
    def stop(self):
        self._q_stop.put(True)
    
    @contextlib.contextmanager
    def _write_jdl(self, job_id):
        template = string.Template(JDL_TEMPLATE)
        
        with tempfile.NamedTemporaryFile("w+") as fh:
            fh.write(template.substitute({
                'executable' : self._runner_path,
                'arguments' : "%s -j %d -r %s" % (self._runner_args, job_id, self._token)
            }))
            
            fh.flush()
            
            yield fh.name
    
    @retry(tries = 3)
    def _launch_pilot(self, job_id):
        with self._write_jdl(job_id) as jdl_file:
            return glite.ce.job.submit(jdl_file, endpoint=self._ce_queue)
    
    def _launch_job(self, job_id):
        with self._glite_ids as glite_ids:
            with self._glite_status as glite_status:
                with self._bt_ids as bt_ids:
                    with self._bt_status as bt_status:
                        
                        pilot_id = self._launch_pilot(job_id)
                        
                        if job_id in bt_ids:
                            old_status = bt_ids[job_id]['status']
                            bt_status[old_status] -= 1
                        
                        bt_ids[job_id]['status'] = bt.Job.Status.QUEUED
                        bt_ids[job_id]['glite_id'] = pilot_id
                        bt_status[bt.Job.Status.QUEUED] += 1
                        
                        glite_ids[pilot_id]['status'] = glite.ce.job.Status.REGISTERED
                        glite_ids[pilot_id]['job_id'] = job_id
                        glite_status[glite.ce.job.Status.REGISTERED] += 1
        return pilot_id
    
    @bt.retry_on_serializable_error
    def _cleanup_job(self, job_id, tb=None):
        with bt.transactional_session(self._session_maker) as session:
            job = session.query(bt.Job).filter_by(id = job_id).one()
            job.cleanup(self._token, tb)
    
    @bt.retry_on_serializable_error
    def _save_glite_id(self, job_id, glite_id):
        with bt.transactional_session(self._session_maker) as session:
            job = session.query(bt.Job).filter_by(id = job_id).one()
            job.tag['bt_glite_id'] = glite_id
    
    @bt.retry_on_serializable_error
    def _reserve_one(self):
        with bt.transactional_session(self._session_maker) as session:
            job = session.query(bt.Job).filter(
                bt.Job.status == bt.Job.Status.QUEUED,
                bt.Job.token == None,
                bt.Job.name.in_(self._allowed_tasks), # @UndefinedVariable
                ~ bt.Job.parents.any(bt.Job.status != bt.Job.Status.DONE) # @UndefinedVariable
            ).first()
            
            if job:
                job.reserve(self._token)
                return job.id
    
    def _launch_pending(self):
        while not self._q_stop.poll():
            job_id = self._reserve_one()
            if job_id:
                try:
                    glite_id = self._launch_job(job_id)
                    self._save_glite_id(job_id, glite_id)
                except (RuntimeError, NoResultFound):
                    tb = ''.join(traceback.format_exception(*sys.exc_info()))
                    try:
                        self._cleanup_job(job_id, tb)
                    except (bt.InvalidStatusException, bt.TokenMismatchException, NoResultFound):
                        pass
                
                self._refresh.put(True)
            else:
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

class BtMonitorThread(threading.Thread):
    """
    This thread uses the BT notifications mechanism to update the status of BT
    jobs.
    
    A dummy element is pushed to 'refresh' queue to signal the main thread to
    update the UI.
    """
    
    def __init__(self, session_maker, bt_ids, bt_status, refresh):
        super(BtMonitorThread, self).__init__(name='bt_monitor')
        self._session_maker = session_maker
        self._bt_ids = bt_ids
        self._bt_status = bt_status
        self._refresh = refresh
        self._q_stop = utils.SelectableQueue()
        
        self._q_changes = bt.Notifications(self._session_maker)
        self._q_changes.listen(self._q_changes.channel.all_job_channels)
    
    def stop(self):
        self._q_stop.put(True)
    
    def _update_job(self, job_id):
        try:
            with bt.transactional_session(self._session_maker) as session:
                job = session.query(bt.Job).filter(bt.Job.id == job_id).one()
                
                with self._bt_ids as bt_ids:
                    with self._bt_status as bt_status:
                        if job.id in bt_ids:
                            old_status = bt_ids[job.id]['status']
                            bt_status[old_status] -= 1
                            bt_status[job.status] += 1
                            bt_ids[job.id]['status'] = job.status
        
        except NoResultFound:
            with self._bt_ids as bt_ids:
                with self._bt_status as bt_status:
                    if job_id in bt_ids:
                        old_status = bt_ids[job_id]['status']
                        del bt_ids[job_id]
                        bt_status[old_status] -= 1
    
    def run(self):
        while not self._q_stop.poll():
            try:
                r, _, _ = select.select([self._q_changes, self._q_stop], [], [])
                
                if self._q_stop in r:
                    continue
                
                if self._q_changes in r:
                    for _, payload in self._q_changes:
                        self._update_job(int(payload))
                        
                        if self._q_stop.poll():
                            break
                    
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
    
    @retry(tries = 3)
    def _init_status(self):
        self._last_event = glite.ce.last_event_id(self._ce_host)
    
    @retry(tries = 3)
    def _pending_events(self):
        return glite.ce.event_query(self._ce_host, self._last_event)
    
    def _update_status(self):
        event = None
        for event in self._pending_events():
            with self._glite_ids as glite_ids:
                with self._glite_status as glite_status:
                    if event['jobId'] in glite_ids:
                        old_status = glite_ids[event['jobId']]['status']
                        glite_status[old_status] -= 1
                        glite_status[event['status']] += 1
                        glite_ids[event['jobId']]['status'] = event['status']
            
            self._last_event = int(event['EventID'])
            
            if self._q_stop.poll():
                break
        
        if event:
            self._refresh.put(True)
    
    def run(self):
        self._update_status()
        
        while not self._q_stop.poll():
            try:
                r, _, _ = select.select([self._q_stop], [], [], GliteMonitorThread.POLL_INTERVAL)
                
                if self._q_stop in r:
                    continue
                
                self._update_status()
            
            except select.error as e:
                if e.args[0] != errno.EINTR:
                    raise
        
        self._q_stop.get()
