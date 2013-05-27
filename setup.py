#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os

from setuptools import setup, find_packages

requires = [
    'argparse',
    'jsonschema',
    'PyYAML',
    'SQLAlchemy >= 0.8',
    'tabulate',
    'termcolor',
    'transaction',
    'zope.sqlalchemy',
],

try:
    from logging.config import dictConfig # @UnusedImport
except ImportError:
    requires.append('logutils')

# Read README and CHANGES files for the long description
here = os.path.abspath(os.path.dirname(__file__))
README  = open(os.path.join(here, 'README.txt')).read()

# Read the version information
execfile(os.path.join(here, 'brownthrower', 'release.py'))

setup(
    name = 'brownthrower',
    version = __version__, # @UndefinedVariable
    packages = find_packages(),
    
    install_requires = requires,
    
    description = "",
    long_description = README,
    author = 'Pau Tallada Crespí',
    author_email = 'pau.tallada@gmail.com',
    maintainer = 'Pau Tallada Crespí',
    maintainer_email = 'pau.tallada@gmail.com',
    url = "http://packages.python.org/brownthrower",
    
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
            'manager = brownthrower.manager.__init__:main',
            'dispatcher.serial = brownthrower.dispatcher.serial:main'
        ],
        'brownthrower.dispatcher' : [
            "dispatcher01 = brownthrower.dispatcher.serial:SerialDispatcher",
        ],
        'brownthrower.task': [
            # math
            'task01 = examples.task.math.add:Add',
            'task02 = examples.task.math.sum:Sum',
            # misc
            'task03 = examples.task.misc.noop:Noop',
            #'misc.hostname         = examples.task.misc.hostname:Hostname',
            #'misc.store_single_env = examples.task.misc.store_single_env:StoreSingleEnv',
        ],
    },
    
    include_package_data=True,
    #zip_safe=True,
)
