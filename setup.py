#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os

from setuptools import setup, find_packages

install_requires = [
    'argparse',
    'glite>=1.9.0',
    'logutils', # Only for Python <= 2.6
    'pyparsing <2.0a0',
    'PyYAML',
    'setuptools',
    'SQLAlchemy >=0.9, <1.0',
    'tabulate',
    'termcolor',
    'trunk',
    'urwid',
]

# Read the version information
here = os.path.abspath(os.path.dirname(__file__))
execfile(os.path.join(here, 'brownthrower', 'release.py'))

setup(
    name = 'brownthrower',
    version = __version__, # @UndefinedVariable
    packages = find_packages(),
    
    install_requires = install_requires,
    
    test_suite = 'nose.collector',
    tests_require = [
        'nose',
        'nose-testconfig',
    ],
    
    description = "Framework for executing jobs with inter-dependencies",
    
    author = 'Pau Tallada Crespí',
    author_email = 'pau.tallada@gmail.com',
    maintainer = 'Pau Tallada Crespí',
    maintainer_email = 'pau.tallada@gmail.com',
    
    #url = "http://packages.python.org/brownthrower",
    
    #license = 'AGPLv3+',
    #keywords = "",
    
    entry_points = {
        'console_scripts' : [
            'brownthrower = brownthrower.manager.__init__:main',
            'runner.serial = brownthrower.runner.serial.__init__:main',
            'dispatcher.static = brownthrower.dispatcher.static.__init__:main',
            'dispatcher.on_demand = brownthrower.dispatcher.on_demand.__init__:main',
        ],
        'brownthrower.task' : [
            'random   = brownthrower.examples.math:Random',
            'add2     = brownthrower.examples.math:Add2',
            'sum4     = brownthrower.examples.math:Sum4',
            'pipe     = brownthrower.examples.misc:Pipe',
            'sleep    = brownthrower.examples.misc:Sleep',
            'environ  = brownthrower.examples.misc:Environ',
        ],
    },
    
    include_package_data=True,
    zip_safe=True,
)
