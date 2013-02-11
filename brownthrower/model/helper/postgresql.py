#!/usr/bin/env python
# -*- coding: utf-8 -*-

from base import BaseHelper
from brownthrower import model
from sqlalchemy import literal_column

class Helper(BaseHelper):
    
    @staticmethod
    def ancestors(job_id, lockmode=False):
        """
        Retrieve all the ancestors of this node using CTE.
        """
        # TODO: improve with identity_map
        l0 = literal_column('0').label('level')
        q_base = model.session.query(model.Job, l0).filter_by(
                     id = job_id
                 ).cte(recursive = True)
        l1 = literal_column('level + 1').label('level')
        q_rec = model.session.query(model.Job, l1).filter(
                    q_base.c.super_id == model.Job.id
                )
        q_cte = q_base.union_all(q_rec)
        
        pending = model.session.query(model.Job).select_from(q_cte).order_by(q_cte.c.level).all()
        
        ancestors = []
        while len(pending):
            ancestors.insert(0, model.session.query(model.Job).filter_by(
                id = pending.pop().id
            ).with_lockmode(lockmode).one())
        
        if len(ancestors) == 0:
            raise model.NoResultFound("There is no Job with id '%d'." % job_id)
        
        return ancestors

def submit(job):
    # lock ancestors
    # if subjobs:
    #     for each subjob:
    #         submit(subjob)
    # else:
    #     status = QUEUED
    # for each ancestor: 
    #     update_status(ancestor)
    pass