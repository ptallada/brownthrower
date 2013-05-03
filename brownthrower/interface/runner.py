#!/usr/bin/env python
# -*- coding: utf-8 -*-

class Runner(object):
    
    def __init__(self, job_id):
        self.job_id = job_id
    
    def prolog(self):
        pass
    
    def run(self, task, inp):
        self.prolog()
        out = task.run(runner=self, inp=inp)
        self.epilog()
        return out
    
    def epilog(self):
        pass
