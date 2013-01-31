#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import shutil
import tempfile

class Runner(object):
    
    def __init__(self, tmp_dir, tmp_prefix):
        self.tmp_dir = tmp_dir
        self.tmp_prefix = tmp_prefix
        
        self.old_cwd = None
        self.cwd = None
    
    def prolog(self):
        self.cwd = tempfile.mkdtemp(prefix=self.tmp_template, dir=self.tmp_dir)
        self.old_cwd = os.getcwd()
        os.chdir(self.cwd)
    
    def run(self, task, config):
        self.prolog()
        task(config, runner=self)
        self.epilog()
    
    def epilog(self):
        os.chdir(self.old_cwd)
        shutil.rmtree(self.cwd)
