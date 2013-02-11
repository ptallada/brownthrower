#!/usr/bin/env python
# -*- coding: utf-8 -*-

from base import BaseHelper
from brownthrower import model

class Helper(BaseHelper):
    
    @staticmethod
    def ancestors(job_id):
        job = model.session.query(model.Job).filter_by(id = job_id).one()
        ancestors = [job]
        
        while job.superjob:
            ancestors.append(job.superjob)
            job = job.superjob
        
        return ancestors