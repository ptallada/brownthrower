#!/usr/bin/env python
# -*- coding: utf-8 -*-

import Rx
import yaml

class Task(object):
    def run(self, *args, **kwargs):
        raise NotImplementedError
    
    def check_arguments(self, *args, **kwargs):
        raise NotImplementedError

class SimpleEnv(Task):
    
    _schema = """
    type: //rec
    required:
        path: //str
        key:  //str
    """
    
    def run(self, config):
        
        import os
        f = open(config['path'], "w")
        f.write("%s" % os.environ[config['key']])
        f.close()
    
    def check_arguments(self, config):
        