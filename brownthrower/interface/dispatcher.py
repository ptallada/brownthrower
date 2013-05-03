#! /usr/bin/env python
# -*- coding: utf-8 -*-

import textwrap

class BaseDispatcher(object):
    @classmethod
    def get_help(cls):
        doc = cls.__doc__.strip().split('\n')
        short = doc[0].strip()
        detail = textwrap.dedent('\n'.join(doc[1:]))
        
        return (short, detail)

class Dispatcher(BaseDispatcher):
    """\
    This MUST be a single line describing this Dispatcher.
    
    The following line MUST be a more detailed description of the operation of
    this Dispatcher, what are its configuration values and its supported
    working environment.
    """
    
    __brownthrower_name__ = 'dispatcher.name'
    
    def run(self):
        """
        Run the dispatcher until it is interrupted or there are no more jobs
        to be executed.
        """
        raise NotImplementedError
