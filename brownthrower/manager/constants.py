#!/usr/bin/env python
# -*- coding: utf-8 -*-

class JobStatus(object):
    STASHED       = 'STASHED'       # The job is in preparation phase. It is being configured and cannot be executed yet.
    READY         = 'READY'         # The job has been configured and its dependencies are already set. Is this state it cannot be modified.
    QUEUED        = 'QUEUED'        # The dispatcher has submitted this job for execution and it is waiting for some resources to be available.
    RUNNING       = 'RUNNING'       # The job is being executed.
    DONE          = 'DONE'          # The job has finished its execution with success.
    CLOSED_DONE   = 'CLOSED_DONE'   # The dispatcher has retrieved the output of this job and it has reached the end of its life.
    FAILED        = 'FAILED'        # The job has finished its execution with some error condition.
    CLOSED_FAILED = 'CLOSED_FAILED' # The dispatcher has retrieved the output of this job and it has reached the end of its life.
    CANCEL        = 'CANCEL'        # The user wants to cancel this job.
    CANCELLING    = 'CANCELLING'    # The dispatcher is already trying to cancel the job.
    CANCELLED     = 'CANCELLED'     # The job has been cancelled.
    
    _links_from = {
        STASHED    : [READY],
        READY      : [QUEUED, CANCELLED],
        QUEUED     : [RUNNING, CANCEL],
        CANCEL     : [CANCELLING],
        RUNNING    : [DONE, FAILED, CANCEL],
        CANCELLING : [DONE, FAILED, CANCELLED],
        DONE       : [CLOSED_DONE],
        FAILED     : [CLOSED_FAILED],
    }
    
    _links_to = {
        READY         : [STASHED],
        QUEUED        : [READY],
        RUNNING       : [QUEUED],
        CANCEL        : [QUEUED, RUNNING],
        CANCELLING    : [CANCEL],
        CANCELLED     : [READY, CANCELLING],
        DONE          : [RUNNING, CANCELLING],
        FAILED        : [RUNNING, CANCELLING],
        CLOSED_DONE   : [DONE],
        CLOSED_FAILED : [FAILED],
    }
    
    @classmethod
    def links_to(cls, status):
        return cls._links_to[status]
