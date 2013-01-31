#!/usr/bin/env python
# -*- coding: utf-8 -*-

class JobStatus(object):
    DELETED     = 'DELETED'     # The job has been deleted.
    STASHED     = 'STASHED'     # The job is in preparation phase. It is being configured and cannot be executed yet.
    READY       = 'READY'       # The job has been configured and its dependencies are already set. It will be executed as soon as possible.
    SUBMIT_FAIL = 'SUBMIT_FAIL' # The dispatcher has been unable to submit this job for execution.
    CANCEL      = 'CANCEL'      # The user has asked to cancel this job.
    QUEUED      = 'QUEUED'      # The dispatcher has submitted this job for execution and it is waiting for some resources to be available.
    RUNNING     = 'RUNNING'     # The job is being executed.
    DONE        = 'DONE'        # The job has finished with exit code == 0.
    FAILED      = 'FAILED'      # The job/runner was unable to complete its execution successfully.
