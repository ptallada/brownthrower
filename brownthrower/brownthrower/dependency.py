#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

from sqlalchemy.schema import Column, ForeignKeyConstraint, PrimaryKeyConstraint
from sqlalchemy.types import Integer

from . import model

log = logging.getLogger('brownthrower.tag')

class Dependency(model.Base):
    __tablename__ = 'dependency'
    __table_args__ = (
        # Primary key
        PrimaryKeyConstraint('parent_job_id', 'child_job_id', name='pk_dependency'),
        # Foreign keys
        # FIXME: This constraints are not useful for top-level jobs
        ForeignKeyConstraint(            ['parent_job_id'],                 ['job.id'], onupdate='CASCADE', ondelete='CASCADE', name= 'fk_dependency_parent'),
        ForeignKeyConstraint(            ['child_job_id'],                  ['job.id'], onupdate='CASCADE', ondelete='CASCADE', name= 'fk_dependency_child'),
        ForeignKeyConstraint(['super_id', 'parent_job_id'], ['job.super_id', 'job.id'], onupdate='CASCADE', ondelete='CASCADE', name= 'fk_dependency_super_parent'),
        ForeignKeyConstraint(['super_id', 'child_job_id'],  ['job.super_id', 'job.id'], onupdate='CASCADE', ondelete='CASCADE', name= 'fk_dependency_super_child'),
    )
    
    # Columns
    _super_id      = Column('super_id',      Integer, nullable=True)
    _parent_job_id = Column('parent_job_id', Integer, nullable=False)
    _child_job_id  = Column('child_job_id',  Integer, nullable=False)
    
    def __repr__(self):
        return u"%s(super_id=%s, parent_job_id=%s, child_job_id=%s)" % (
            self.__class__.__name__,
            repr(self._super_id),
            repr(self._parent_job_id),
            repr(self._child_job_id),
        )