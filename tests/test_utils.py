'''
Test functions for the "utils" module.
'''

__author__  = ['Miguel Ramos Pernas']
__email__   = ['miguel.ramos.pernas@cern.ch']

# Python
import os

# Local
import stepped_job


def test_create_dir( tmpdir ):
    '''
    Test for "create_dir"
    '''
    # Create another directory
    path = tmpdir.join('dummy').strpath

    stepped_job.utils.create_dir(path)

    assert os.path.exists(path)

    # Check that, if called many times, the directories have different
    # numbers.
    for i in range(3):
        stepped_job.utils.create_dir(path)
    assert list(sorted(map(int, os.listdir(path)))) == list(range(4))


def test_merge_dicts():
    '''
    Test for "merge_dicts"
    '''
    # Test the merging procedure
    a = {'a': 1, 'b': 2}
    b = {'c': 3, 'd': 4}
    c = {'e': 5, 'f': 6}

    res = {'a': 1, 'b': 2, 'c': 3, 'd': 4, 'e': 5, 'f': 6}

    assert set(stepped_job.utils.merge_dicts(a, b, c).items()) == set(res.items())

    # Test that it overwrites values
    a = {'a': 1, 'b': 2}
    b = {'a': 3, 'd': 4}
    c = {'b': 3, 'c': 4}

    res = {'a': 3, 'b': 3, 'c': 4, 'd': 4}

    assert set(stepped_job.utils.merge_dicts(a, b, c).items()) == set(res.items())
