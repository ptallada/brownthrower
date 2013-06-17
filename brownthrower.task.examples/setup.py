#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os

from setuptools import setup, find_packages

install_requires = [
    'brownthrower',
    'logutils', # Only for Python <= 2.6
    'setuptools',
]

# Read README and CHANGES files for the long description
here = os.path.abspath(os.path.dirname(__file__))
#README  = open(os.path.join(here, 'README.txt')).read()

# Read the version information
execfile(os.path.join(here, 'brownthrower', 'task', 'examples', 'release.py'))

setup(
    name = 'brownthrower.task.examples',
    version = __version__, # @UndefinedVariable
    packages = find_packages(),
    namespace_packages = ['brownthrower', 'brownthrower.task'],
    
    install_requires = install_requires,
    
    description = "Example brownthrower tasks for testing and learning",
    #long_description = README,
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
        'brownthrower.task' : [
            # math
            'task01 = brownthrower.task.examples.math.add:Add',
            'task02 = brownthrower.task.examples.math.sum:Sum',
            # misc
            'task03 = brownthrower.task.examples.misc.noop:Noop',
        ],
    },
    
    include_package_data=True,
    #zip_safe=True,
)
