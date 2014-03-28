#! /usr/bin/env python
# -*- coding: utf-8 -*-

import itertools

import brownthrower as bt

from nose.tools import raises
from .base import BaseTest, ExampleJob

class TestJobBase(BaseTest):
    pass

class TestCreate(TestJobBase):
    def test_empty(self, **kwargs):
        j = ExampleJob(**kwargs)
        assert isinstance(j, ExampleJob)
        assert j.status == bt.Job.Status.STASHED
        assert j.ts_created is not None
        print repr(j)
        print str(j)
        return j
    
    def test_readonly_columns(self, **kwargs):
        @raises(AttributeError)
        def check_protected(field):
            ExampleJob(**dict({field: self.randint()}))
        
        columns = [
            'id',
            'super_id',
            'task',
            'status',
            'config',
            'input',
            'output',
            'ts_created',
            'ts_queued',
            'ts_started',
            'ts_ended'
        ]
        
        for column in columns:
            yield check_protected, column

class TestSuperSub(TestJobBase):
    @raises(ValueError)
    def test_set_self_as_superjob(self, **kwargs):
        j1 = TestCreate().test_empty(**kwargs)
        j1.superjob = j1
    
    @raises(ValueError)
    def test_add_self_as_subjob(self, **kwargs):
        j1 = TestCreate().test_empty(**kwargs)
        j1.subjobs.add(j1)
    
    def test_set_processing_superjob_to_stashed_subjob(self, **kwargs):
        j1 = TestCreate().test_empty(**kwargs)
        j2 = TestCreate().test_empty(**kwargs)
        j1._status = bt.Job.Status.PROCESSING
        
        assert j1.status == bt.Job.Status.PROCESSING
        assert j2.status == bt.Job.Status.STASHED
        
        j2.superjob = j1
        
        assert j2.superjob == j1
        assert j2 in j1.subjobs
        
        j2.superjob = None
        
        assert j2.superjob == None
        assert not j1.subjobs
    
    def test_add_stashed_subjob_to_processing_superjob(self, **kwargs):
        j1 = TestCreate().test_empty(**kwargs)
        j2 = TestCreate().test_empty(**kwargs)
        j1._status = bt.Job.Status.PROCESSING
         
        assert j1.status == bt.Job.Status.PROCESSING
        assert j2.status == bt.Job.Status.STASHED
         
        j1.subjobs.add(j2)
        
        assert j2.superjob == j1
        assert j2 in j1.subjobs
        
        j1.subjobs.remove(j2)
        
        assert j2.superjob == None
        assert not j1.subjobs
    
    def test_invalid_status_super_sub(self, **kwargs):
        @raises(bt.InvalidStatusException)
        def check_status_super(st_super, st_sub):
            j1 = TestCreate().test_empty(**kwargs)
            j2 = TestCreate().test_empty(**kwargs)
            j1._status = st_super
            j2._status = st_sub
            
            j2.superjob = j1
        
        @raises(bt.InvalidStatusException)
        def check_status_sub(st_super, st_sub):
            j1 = TestCreate().test_empty(**kwargs)
            j2 = TestCreate().test_empty(**kwargs)
            j1._status = st_super
            j2._status = st_sub
            
            j1.subjobs.add(j2)
        
        sts_super = [
            bt.Job.Status.CANCELLED,
            bt.Job.Status.CANCELLING,
            bt.Job.Status.DONE,
            bt.Job.Status.FAILED,
            bt.Job.Status.FAILING,
            bt.Job.Status.STASHED,
            bt.Job.Status.QUEUED,
        ]
        
        sts_sub = [
            bt.Job.Status.CANCELLED,
            bt.Job.Status.CANCELLING,
            bt.Job.Status.DONE,
            bt.Job.Status.FAILED,
            bt.Job.Status.FAILING,
            bt.Job.Status.PROCESSING,
            bt.Job.Status.QUEUED,
        ]
        
        for st_super, st_sub in itertools.product(sts_super, sts_sub):
            yield check_status_super, st_super, st_sub
            yield check_status_sub, st_super, st_sub 

class TestParents(TestJobBase):
    @raises(ValueError)
    def test_add_self_as_parent(self, **kwargs):
        j1 = TestCreate().test_empty(**kwargs)
        j1.parents.add(j1)
    
    @raises(ValueError)
    def test_add_self_as_child(self, **kwargs):
        j1 = TestCreate().test_empty(**kwargs)
        j1.children.add(j1)
    
    def test_add_stashed_as_parent(self, **kwargs):
        j1 = TestCreate().test_empty(**kwargs)
        j2 = TestCreate().test_empty(**kwargs)
        
        assert j2.status == bt.Job.Status.STASHED
        
        j2.parents.add(j1)
        
        assert j1 in j2.parents
        assert j2 in j1.children
        
        j2.parents.remove(j1)
        
        assert not j2.parents
        assert not j1.children
    
    def test_add_stashed_as_child(self, **kwargs):
        j1 = TestCreate().test_empty(**kwargs)
        j2 = TestCreate().test_empty(**kwargs)
        
        assert j2.status == bt.Job.Status.STASHED
        
        j1.children.add(j2)
        
        assert j2 in j1.children
        assert j1 in j2.parents
        
        j1.children.remove(j2)
        
        assert not j1.children
        assert not j2.parents
    
    def test_invalid_status(self, **kwargs):
        @raises(bt.InvalidStatusException)
        def check_status_parent(status):
            j1 = TestCreate().test_empty(**kwargs)
            j2 = TestCreate().test_empty(**kwargs)
            j2._status = status
            
            j2.parents.add(j1)
        
        @raises(bt.InvalidStatusException)
        def check_status_children(status):
            j1 = TestCreate().test_empty(**kwargs)
            j2 = TestCreate().test_empty(**kwargs)
            j2._status = status
            
            j1.children.add(j2)
        
        status = [
            bt.Job.Status.CANCELLED,
            bt.Job.Status.CANCELLING,
            bt.Job.Status.DONE,
            bt.Job.Status.FAILED,
            bt.Job.Status.FAILING,
            bt.Job.Status.PROCESSING,
            bt.Job.Status.QUEUED,
        ]
        
        for st in status:
            yield check_status_parent, st
            yield check_status_children, st

class TestTag(TestJobBase):
    def test_tag(self, **kwargs):
        j1 = TestCreate().test_empty(**kwargs)
        
        j1.tag['test'] = self.randint()
        print j1.tag['test']
        
        assert 'test' in j1.tag
        
        del j1.tag['test']
        
        assert not j1.tag
