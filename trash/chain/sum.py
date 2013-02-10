#! /usr/bin/env python
# -*- coding: utf-8 -*-

from itertools import izip_longest, islice

from brownthrower import interface

class Sum(interface.Chain):
    """\
    Calculate the sum of the input.
    
    Return the sum of all the inputs. This Chain generates its output building
    a chain of 'add' Tasks.
    """
    
    config_schema = """\
    {
        "type"     : "null",
        "$schema"  : "http://json-schema.org/draft-03/schema",
        "required" : true
    }
    """
    
    input_schema = """\
    {
        "type"     : "array",
        "$schema"  : "http://json-schema.org/draft-03/schema",
        "required" : true,
        "minItems" : 2,
        "items"    : {
            "type"     : "integer",
            "required" : true
        }
    }
    """
    
    output_schema = """\
    {
        "type"                 : "integer",
        "$schema"              : "http://json-schema.org/draft-03/schema",
        "required"             : true
    }
    """
    
    config_sample = """\
        # Nothing is required for this job.
    """
    
    input_sample = """\
        # An unbounded array of integers
        [ 1, 2, 3, 4 ]
    """
    
    output_sample = """\
        # The sum of the input
        42
    """
    
    def prolog(self, tasks, config, inp):
        task = tasks['math.add']
        noop = tasks['misc.noop']
        
        dependencies = []
        pending = []
        
        # First pass for initial tasks
        for (i1, i2) in izip_longest(islice(inp, 0, None, 2), islice(inp, 1, None, 2)):
            if i2 != None:
                t = task(config)
                dependencies.append((i1, i2), t)
            else:
                t = noop(config)
                dependencies.append(i1, t)
            pending.append(t)
            
        # Now group the tasks two by two
        inp = pending
        while len(inp) > 1:
            for (t1, t2) in izip_longest(islice(inp, 0, None, 2), islice(inp, 1, None, 2)):
                if t2 != None:
                    t = task(config)
                    dependencies.append(t1, t)
                    dependencies.append(t2, t)
                else:
                    t = t1
                pending.append(t)
            inp = pending
        return dependencies
        
    def epilog(self, config, out):
        return out[0]