#!/usr/bin/env python
# -*- coding: utf-8 -*-

class JobStatus(object):
    STASHED     = 'STASHED'     # The job is in preparation phase. It is being configured and cannot be executed yet.
    QUEUED      = 'QUEUED'      # The job has been configured and its dependencies are already set. It will be executed as soon as possible.
    CANCELLING  = 'CANCELLING'  # The user has asked to cancel this job.
    CANCELLED   = 'CANCELLED'   # The job has been interrupted. No inner job has failed.
    PROCESSING  = 'PROCESSING'  # The job is being processed.
    DONE        = 'DONE'        # The job has finished successfully.
    FAILING     = 'FAILING'     # The job is still being processed and some inner job has failed.
    FAILED      = 'FAILED'      # The job did not finish succesfully.
    PROLOG_FAIL = 'PROLOG_FAIL' # The prolog has failed to finish successfully.
    EPILOG_FAIL = 'EPILOG_FAIL' # The epilog has failed to finish successfully.
