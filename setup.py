#!/usr/bin/env python
# -*- coding: utf-8 -*-

#import os
from setuptools import setup, find_packages

# Read README file for the long description
#here = os.path.abspath(os.path.dirname(__file__))
#README = open(os.path.join(here, 'README')).read()

setup(
    name = 'brownthrower',
    version = '0.1dev',
    packages = find_packages(),
    
    install_requires = [
        'Python >= 2.7',
        'PyYAML',
        'jsonschema',
        'SQLAlchemy < 0.7.999',
        'PrettyTable',
        'termcolor',
    ],
    
    #description = "",
    #long_description = README,
    author = 'Pau Tallada Crespí',
    author_email = 'pau.tallada@gmail.com',
    maintainer = 'Pau Tallada Crespí',
    maintainer_email = 'pau.tallada@gmail.com',
    #url = "http://packages.python.org/multivac",
    
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
        ],
        'brownthrower.dispatcher' : [
            "serial = brownthrower.dispatcher.serial:SerialDispatcher",
        ],
        'brownthrower.task': [
            # math
            'math.add  = examples.math.task.add:Add',
            # misc
            'misc.noop             = examples.misc.task.noop:Noop',
            'misc.hostname         = examples.misc.task.hostname:Hostname',
            'misc.store_single_env = examples.misc.task.store_single_env:StoreSingleEnv',
        ],
        'brownthrower.chain': [
            # math
            'math.sum  = examples.math.chain.sum:Sum',
        ],
    },
    
    include_package_data=True,
    #zip_safe=True,
)
