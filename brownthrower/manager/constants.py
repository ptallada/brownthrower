#!/usr/bin/env python
# -*- coding: utf-8 -*-

class JobStatus(object):
    STASHED         = 'STASHED'         # The job is in preparation phase. It is being configured and cannot be executed yet.
    STEADY          = 'STEADY'          # The job has been configured and its dependencies are already set. It will be executed as soon as possible.
    SUBMIT_FAIL     = 'SUBMIT_FAIL'     # The dispatcher has been unable to submit this job for execution.
    QUEUED          = 'QUEUED'          # The dispatcher has submitted this job for execution and it is waiting for some resources to be available.
    RUNNING         = 'RUNNING'         # The job is being executed.
    ABORTED         = 'ABORTED'         # The job has failed to complete its execution.
    CANCEL          = 'CANCEL'          # The user has asked to cancel this job.
    CANCELLING      = 'CANCELLING'      # The dispatcher is trying to cancel this job
    CANCELLED       = 'CANCELLED'       # The job has been cancelled.
    SUCCESS         = 'SUCCESS'         # The job has finished with exit code == 0
    FAILED          = 'FAILED'          # The job has finished with exit code != 0
    CLEARED_SUCCESS = 'CLEARED_SUCCESS' # The dispatcher has retrieved the output of this job.
    CLEARED_FAILED  = 'CLEARED_FAILED'  # The dispatcher has retrieved the output of this job.
    OUTPUT_LOST     = 'OUTPUT_LOST'     # The dispatcher could not retrieve the output of this job.
    DELETED         = 'DELETED'         # The job has been deleted.
    
    _manager_links_to = {
        STASHED         : [ABORTED, CANCELLED, STEADY, SUBMIT_FAIL],
        STEADY          : [ABORTED, CANCELLED, STASHED, SUBMIT_FAIL],
        SUBMIT_FAIL     : [],
        QUEUED          : [],
        RUNNING         : [],
        ABORTED         : [],
        CANCEL          : [QUEUED, RUNNING],
        CANCELLING      : [],
        CANCELLED       : [STEADY],
        SUCCESS         : [],
        FAILED          : [],
        CLEARED_SUCCESS : [],
        CLEARED_FAILED  : [],
        OUTPUT_LOST     : [],
        DELETED         : [ABORTED, CANCELLED, CLEARED_FAILED, CLEARED_SUCCESS, OUTPUT_LOST, STEADY, STASHED],
    }
    
    @classmethod
    def manager_links_to(cls, status):
        return cls._manager_links_to[status]
