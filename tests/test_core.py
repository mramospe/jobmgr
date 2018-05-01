'''
Test functions for the "core" module.
'''

__author__  = ['Miguel Ramos Pernas']
__email__   = ['miguel.ramos.pernas@cern.ch']

# Python
import pytest

# Local
import stepped_job
from . import using_directory


def test_job_manager():
    '''
    Tests for the JobManager instance
    '''
    # Test singleton behaviour
    assert stepped_job.JobManager() is stepped_job.JobManager()


def test_manager():
    '''
    Test the function to get the job manager.
    '''
    # Test the function "manager"
    assert stepped_job.manager() is stepped_job.JobManager()


@using_directory('test_job')
def test_job():
    '''
    Test the behaviour of the Job instance.
    '''
    # Raise error if two steps have the same name
    job = stepped_job.Job('test_job')
    job.add_step('fail', 'python', ['-c', 'print()'], data_regex='.*txt')
    with pytest.raises(RuntimeError):
        job.add_step('fail', 'python', ['-c', 'print()'], data_regex='.*txt')

    # Create a job and run it
    job = stepped_job.Job('test_job')

    executable = 'python'

    opts_create = [
        '-c',
        'with open("dummy.txt", "wt") as f: f.write("testing\\n")'
        ]

    job.add_step('create', executable, opts_create, data_regex='.*txt')

    opts_consume = [
        '-c',
        'import sys; f = open(sys.argv[1]); print(f.read())'
        ]

    job.add_step('consume', executable, opts_consume, data_regex='.*txt')

    job.start()

    job.wait()

    assert job.status() == stepped_job.core.status.terminated

    # If one step fails, it should kill the rest
    job = stepped_job.Job('test_job')

    executable = 'python'

    opts_create = ['-c', 'cause error']

    job.add_step('create', executable, opts_create, data_regex='.*txt')

    opts_consume = ['-c', 'print("should run fine")']

    job.add_step('consume', executable, opts_consume, data_regex='.*txt')

    job.start()

    job.wait()

    for s in job.steps:
        print(s.status())

    assert all(map(lambda s: s.status() == stepped_job.core.status.killed,
                   job.steps))

    assert job.status() == stepped_job.core.status.killed

    # Test killing a job
    job = stepped_job.Job('test_job')

    executable = 'python'

    opts_create = [
        '-c',
        'open("dummy.txt", "wt").write("testing\\n"); while True: pass'
        ]

    job.add_step('create', executable, opts_create, data_regex='.*txt')

    opts_consume = [
        '-c',
        'import sys; f = open(sys.argv[1]); print(f.read())'
        ]

    job.add_step('consume', executable, opts_consume, data_regex='.*txt')

    job.start()

    job.kill()

    assert job.status() == stepped_job.core.status.killed
