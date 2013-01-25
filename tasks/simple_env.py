#!/usr/bin/env python
# -*- coding: utf-8 -*-

import jsonschema

class Task(object):
    def run(self, *args, **kwargs):
        raise NotImplementedError
    
    def check_arguments(self, *args, **kwargs):
        raise NotImplementedError

class SimpleEnv(object):
    
    _schema = """\
    {
        "type": "object", 
        "$schema": "http://json-schema.org/draft-03/schema", 
        "required": true, 
        "additionalProperties": false, 
        "properties": {
            "key": {
                "type": "string", 
                "required": true
            }, 
            "path": {
                "type": "string", 
                "required": true
            }
        }
    }
    """
    
    _sample = """\
    {
        /* Environment variable to be printed */
        "key"  : "LD_LIBRARY_PATH",
        
        /* Path to the output file where the environment variable
           has to be printed */
        "path" : "output.txt"
    }
    """
    
    def run(self, config):
        self.check_arguments(config)
        
        import os
        f = open(config['path'], "w")
        f.write("%s" % os.environ[config['key']])
        f.close()
    
    def check_arguments(self, config):
        jsonschema.validate(config, _schema)
    
    def get_sample_arguments(self):
        return textwrap.dedent(self._sample)
