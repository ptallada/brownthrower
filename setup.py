#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os

from setuptools import setup, find_packages

install_requires = [
    'argparse',
    'logutils', # Only for Python <= 2.6
    'pydevd',
    'pyparsing >=2.0, <3.0a0',
    'PyYAML',
    'setuptools',
    'SQLAlchemy >=1.0, <2.0a0',
    'tabulate',
    'termcolor',
    'trunk',
]

# Read the version information
version = {}
here = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(here, 'brownthrower', 'release.py')) as fp:
    exec(fp.read(), version)

setup(
    name = 'brownthrower',
    version = version['__version__'],
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
