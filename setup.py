#!/usr/bin/env python
'''
Setup script for the stepped_job package
'''

__author__ = 'Miguel Ramos Pernas'
__email__  = 'miguel.ramos.pernas@cern.ch'


# Python
import os
from setuptools import setup, find_packages

#
# Version of the package. Before a new release is made
# just the "version_info" must be changed. The options
# for the fourth tag are "dev", "alpha", "beta",
# "cand", "final" or "post".
#
version_info = (0, 0, 0, 'dev', 0)

tag = version_info[3]

if tag != 'final':
    if tag == 'alpha':
        frmt = '{}a{}'
    elif tag == 'beta':
        frmt = '{}b{}'
    elif tag == 'cand':
        frmt = '{}rc{}'
    elif tag == 'post':
        frmt = '{}.post{}'
    elif tag == 'dev':
        frmt = '{}.dev{}'
    else:
        raise ValueError('Unable to parse version information')

version = frmt.format('.'.join(map(str, version_info[:3])), version_info[4])

# Setup function
setup(

    name = 'stepped_job',

    version = version,

    description = 'Environment to create jobs with different steps, '\
        'where the output of one step is used as input for the next one',

    # Read the long description from the README
    long_description = open('README.rst').read(),

    # Keywords to search for the package
    keywords = 'physics hep statistics plotting',

    # Find all the packages in this directory
    packages = find_packages(),

    # Install scripts
    scripts = ['scripts/{}'.format(f) for f in os.listdir('scripts')],

    # Requisites
    install_requires = ['pytest'],

    # Test requirements
    setup_requires = ['pytest-runner'],

    tests_require = ['pytest'],
    )


# Create a module with the versions
version_file = open('stepped_job/version.py', 'wt')
version_file.write("""\
'''
Auto-generated module holding the version of the stepped_job package
'''

__version__ = "{}"
__version_info__ = {}

__all__ = ['__version__', '__version_info__']
""".format(version, version_info))
version_file.close()
