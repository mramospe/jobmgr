'''
Test functions for the "jobs" module.
'''

__author__  = ['Miguel Ramos Pernas']
__email__   = ['miguel.ramos.pernas@cern.ch']

# Python
import pytest

# Local
import stepped_job
from . import using_directory


@using_directory('test_job_base')
def test_job_base():
    '''
    Test the behaviour of the JobBase instance.
    '''
    j0 = stepped_job.JobBase('test_job_base')

    assert j0 in stepped_job.JobManager().values()

    j1 = stepped_job.JobBase('test_job_base', register=False)

    assert j1 not in stepped_job.JobManager().values()


@using_directory('test_job')
def test_job():
    '''
    Test the behaviour of the Job instance.
    '''
    # Test registered job
    j0 = stepped_job.Job('python', ['-c', 'print("testing")'], 'test_job')

    assert j0 in stepped_job.JobManager().values()

    j0.start()

    # Test unregistered job
    j1 = stepped_job.Job('python', ['-c', 'print("testing")'], 'test_job', register=None)

    assert j1 not in stepped_job.JobManager().values()

    j1.start()

    # Wait for completion
    for j in (j0, j1):
        j.wait()
        assert j.status() == stepped_job.StatusCode.terminated


@using_directory('test_stepped_job')
def test_stepped_job():
    '''
    Test the behaviour of the SteppedJob instance.
    '''
    # Raise error if two steps have the same name
    job = stepped_job.SteppedJob('test_stepped_job')
    job.add_step('fail', 'python', ['-c', 'print()'], data_regex='.*txt')
    with pytest.raises(RuntimeError):
        job.add_step('fail', 'python', ['-c', 'print()'], data_regex='.*txt')

    # Create a job and run it
    job = stepped_job.SteppedJob('test_stepped_job')

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

    assert job.status() == stepped_job.core.StatusCode.terminated

    # If one step fails, it should kill the rest
    job = stepped_job.SteppedJob('test_stepped_job')

    executable = 'python'

    opts_create = ['-c', 'cause error']

    job.add_step('create', executable, opts_create, data_regex='.*txt')

    opts_consume = ['-c', 'print("should run fine")']

    job.add_step('consume', executable, opts_consume, data_regex='.*txt')

    job.start()

    job.wait()

    for s in job.steps:
        print(s.status())

    assert all(map(lambda s: s.status() == stepped_job.core.StatusCode.killed,
                   job.steps))

    assert job.status() == stepped_job.core.StatusCode.killed

    # Test killing a job
    job = stepped_job.SteppedJob('test_stepped_job')

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

    assert job.status() == stepped_job.core.StatusCode.killed
