'''
Test functions for the "utils" module.
'''

__author__  = ['Miguel Ramos Pernas']
__email__   = ['miguel.ramos.pernas@cern.ch']

# Python
import os

# Local
import stepped_job
from . import rm_tmp


def test_create_dir():
    '''
    Test for "create_dir"
    '''
    # Check the name of the default directory
    stepped_job.utils.create_dir()
    rm_tmp(stepped_job.utils.__default_dir__)
    assert os.path.exists(stepped_job.utils.__default_dir__)

    # Create another directory
    new_dir = 'dummy'

    stepped_job.utils.create_dir(new_dir)
    rm_tmp(new_dir)
    assert os.path.exists(new_dir)

    # Check that, if called many times, the directories have different
    # numbers.
    for i in range(3):
        stepped_job.utils.create_dir(new_dir)
    assert list(sorted(map(int, os.listdir(new_dir)))) == list(range(4))
