#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
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
    
    include_package_data=True,
    #zip_safe=True,
)
