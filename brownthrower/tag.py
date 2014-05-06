#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

from sqlalchemy.schema import Column, ForeignKeyConstraint, Index, PrimaryKeyConstraint
from sqlalchemy.types import Integer, String, Text

from . import model

log = logging.getLogger('brownthrower.tag')

class Tag(model.Base):
    __tablename__ = 'tag'
    __table_args__ = (
        # Primary key
        PrimaryKeyConstraint('job_id', 'name', name = 'pk_tag'),
        # Foreign keys
        ForeignKeyConstraint(['job_id'], ['job.id'], onupdate='CASCADE', ondelete='CASCADE', name='fk_tag_job'),
        # Indexes
        Index('ix_tag_name_value', 'name', 'value'),
    )
    
    # Columns
    _job_id = Column('job_id', Integer,    nullable=False)
    _name   = Column('name',   String(20), nullable=False)
    _value  = Column('value',  Text,       nullable=True)
    
    def __repr__(self):
        return u"%s(job_id=%s, name=%s, value=%s)" % (
            self.__class__.__name__,
            repr(self._job_id),
            repr(self._name),
            repr(self._value),
        )