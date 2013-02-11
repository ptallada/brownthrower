#!/usr/bin/env python
# -*- coding: utf-8 -*-

class BaseHelper(object):
    """
    Base static class for database abstraction methods.
    
    Each supported database shall inherit from this class and implement all of
    its methods.
    """
    
    @staticmethod
    def ancestors(job_id, lockmode=False):
        """
        Retrieve all the ancestors of the supplied job ID, starting from itself.
        
        @param job_id: job identifier to retrieve its ancestors.
        @type job_id: int
        @param lockmode: locking mode to use with results
        @type lockmode: str
        
        @return: A list of all the ancestors, ordered by proximity.
        @rtype: list<L{Job}>
        """
        raise NotImplementedError
    
    @staticmethod
    def update(job):
        pass