#!/usr/bin/env python
# -*- coding: utf-8 -*-

class JobStatus(object):
    STASHED     = 'STASHED'     # The job is in preparation phase. It is being configured and cannot be executed yet.
    READY       = 'READY'       # The job has been configured and its dependencies are already set. It will be executed as soon as possible.
    SUBMIT_FAIL = 'SUBMIT_FAIL' # The dispatcher has been unable to submit this job for execution.
    CANCEL      = 'CANCEL'      # The user has asked to cancel this job.
    QUEUED      = 'QUEUED'      # The dispatcher has submitted this job for execution and it is waiting for some resources to be available.
    RUNNING     = 'RUNNING'     # The job is being executed.
    DONE        = 'DONE'        # The job has finished with exit code == 0.
    FAILED      = 'FAILED'      # The job/runner was unable to complete its execution successfully.

class ClusterStatus(object):
    DELETED     = 'DELETED'     # The cluster has been deleted.
    STASHED     = 'STASHED'     # The cluster is in preparation phase. It is being configured and cannot be executed yet.
    READY       = 'READY'       # The cluster has been configured and its dependencies are already set. It will be executed as soon as possible.
    DEPLOYED    = 'DEPLOYED'    # The prolog has been executed successfully and the job graph has been generated.
    PROLOG_FAIL = 'PROLOG_FAIL' # The prolog has failed to finish successfully
    CANCELLING  = 'CANCELLING'  # The user has asked to cancel this cluster.
    CANCELLED   = 'CANCELLED'   # The cluster has been interrupted. No job has failed.
    FAILING     = 'FAILING'     # The cluster is still being processed and some job has failed.
    PROCESSING  = 'PROCESSING'  # The cluster is being processed. No job has failed.
    DONE        = 'DONE'        # All the jobs have finished successfully.
    FAILED      = 'FAILED'      # All the jobs have finished and some have failed.
