'''
Test functions for the "jobs" module.
'''

__author__  = ['Miguel Ramos Pernas']
__email__   = ['miguel.ramos.pernas@cern.ch']

# Python
import pytest

# Local
import stepped_job


def test_job_base( tmpdir ):
    '''
    Test the behaviour of the JobBase instance.
    '''
    path = tmpdir.join('test_job').strpath

    j0 = stepped_job.JobBase(path)

    assert j0 in stepped_job.JobManager()

    j1 = stepped_job.JobBase(path, registry=stepped_job.JobRegistry())

    assert j1 not in stepped_job.JobManager()


def test_job( tmpdir ):
    '''
    Test the behaviour of the Job instance.
    '''
    path = tmpdir.join('test_job').strpath

    # Test registered job
    j0 = stepped_job.Job('python', ['-c', 'print("testing")'], path)

    assert j0 in stepped_job.JobManager()

    j0.start()

    # Test job with a different registry
    j1 = stepped_job.Job('python', ['-c', 'print("testing")'], path, registry=stepped_job.JobRegistry())

    assert j1 not in stepped_job.JobManager()

    j1.start()

    # Wait for completion
    for j in (j0, j1):
        j.wait()
        assert j.status() == stepped_job.StatusCode.terminated

    # Test killing a job
    j2 = stepped_job.Job('python', ['-c', 'while True: pass'], path)
    j2.start()
    j2.kill()

    assert j2.status() == stepped_job.StatusCode.killed


def test_stepped_job( tmpdir ):
    '''
    Test the behaviour of the SteppedJob instance.
    '''
    path = tmpdir.join('test_job').strpath

    # Raise error if two steps have the same name
    job = stepped_job.SteppedJob(path)

    job.add_step('fail', 'python', ['-c', 'print()'], data_regex='.*txt')

    with pytest.raises(RuntimeError):
        job.add_step('fail', 'python', ['-c', 'print()'], data_regex='.*txt')

    # Create a job and run it
    job = stepped_job.SteppedJob(path)

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
    job = stepped_job.SteppedJob(path)

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
    job = stepped_job.SteppedJob(path)

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
