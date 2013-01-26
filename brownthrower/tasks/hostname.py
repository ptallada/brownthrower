#!/usr/bin/env python
# -*- coding: utf-8 -*-

import jsonschema
import textwrap

_schema = {
    "type"    : "null",
    "$schema" : "http://json-schema.org/draft-03/schema",
    "required": True
}

_sample = """\
    # No configuration is required for this job.
"""

_help = (
    """\
    Returns the hostname of the execution host.""",
    """\
    This job gets the hostname of the host in which is been executed and returns it as its result.
    It does not require any parameter.
    """)

def check_arguments(config):
    jsonschema.validate(config, _schema)

def get_template():
    return textwrap.dedent(_sample)

def get_help():
    return (textwrap.dedent(_help[0]), textwrap.dedent(_help[1]))

def run(config, runner):
    check_arguments(config)
    
    import socket
    
    return sockect.gethostname()
