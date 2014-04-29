#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os

from setuptools import setup

install_requires = [
    'argparse',
    'glite>=1.8.0',
    'logutils', # Only for Python <= 2.6
    'PyYAML',
    'repoze.sendmail',
    'setuptools',
    'SQLAlchemy >=0.9, <1.0',
    'tabulate',
    'termcolor',
    'transaction',
    'zope.sqlalchemy',
]

# Read README and CHANGES files for the long description
here = os.path.abspath(os.path.dirname(__file__))
README  = open(os.path.join(here, 'README.txt')).read()

# Read the version information
execfile(os.path.join(here, 'brownthrower', 'release.py'))

setup(
    name = 'brownthrower',
    version = __version__, # @UndefinedVariable
    packages = [ 'brownthrower' ],
    namespace_packages = ['brownthrower'],
    
    install_requires = install_requires,
    
    test_suite = 'nose.collector',
    tests_require = [
        'nose',
        'nose-testconfig',
    ],
    
    description = "Framework for executing jobs with inter-dependencies",
    long_description = README,
    author = 'Pau Tallada Crespí',
    author_email = 'pau.tallada@gmail.com',
    maintainer = 'Pau Tallada Crespí',
    maintainer_email = 'pau.tallada@gmail.com',
    #url = "http://packages.python.org/brownthrower",
    
    #license = 'AGPLv3+',
    #keywords = "",
    #classifiers = [
    #    "Development Status :: 5 - Production/Stable",
    #    "Intended Audience :: Developers",
    #    "License :: OSI Approved :: GNU Affero General Public License v3 or later (AGPLv3+)",
    #    "Programming Language :: Python",
    #    "Operating System :: OS Independent",
    #    "Topic :: Database",
    #    "Topic :: Software Development :: Libraries :: Python Modules",
    #    "Topic :: Utilities"
    #],
    
    # other arguments here...
    entry_points = {
        'console_scripts' : [
            'brownthrower = brownthrower.manager.__init__:main',
            'runner.serial = brownthrower.runner.serial.__init__:main',
            #'dispatcher.static = brownthrower.dispatcher.static.__init__:main',
        ],
        'brownthrower.task' : [
            'random = brownthrower.examples.math:Random',
        ],
    },
    
    include_package_data=True,
    #zip_safe=True,
)
