'''
Test functions for the "jobs" module.
'''

__author__  = ['Miguel Ramos Pernas']
__email__   = ['miguel.ramos.pernas@cern.ch']

# Python
import pytest

# Local
import jobmgr


def test_job_base( tmpdir ):
    '''
    Test the behaviour of the JobBase instance.
    '''
    path = tmpdir.join('test_job_base').strpath

    with jobmgr.ContextManager() as jobs:

        j0 = jobmgr.JobBase(path)

        assert j0 in jobs

        j1 = jobmgr.JobBase(path, registry=jobmgr.JobRegistry())

        assert j1 not in jobs


def test_job_in_registry( tmpdir ):
    '''
    Test the behaviour of the Job instance.
    '''
    path = tmpdir.join('test_job_in_registry').strpath

    with jobmgr.ContextManager() as jobs:

        # Test registered job
        j0 = jobmgr.Job('python', ['-c', 'print("testing")'], path)

        assert j0 in jobs

        # Test job with a different registry
        reg = jobmgr.JobRegistry()

        j1 = jobmgr.Job('python', ['-c', 'print("testing")'], path, registry=reg)

        assert j1 not in jobs
        assert j1 in reg


def test_job_completion( tmpdir ):
    '''
    Test the behaviour of the Job instance.
    '''
    path = tmpdir.join('test_job_completion').strpath

    reg = jobmgr.JobRegistry()

    j0 = jobmgr.Job('python', ['-c', 'print("testing")'], path, registry=reg)
    j1 = jobmgr.Job('python', ['-c', 'while True: pass'], path, registry=reg)

    for j in (j0, j1):
        j.start()

    j0.wait()
    j1.kill()

    reg.watchdog.stop()

    assert j0.status() == jobmgr.StatusCode.terminated
    assert j1.status() == jobmgr.StatusCode.killed


def test_stepped_job_registry( tmpdir ):
    '''
    Test for the SteppedJob class. Checks between SteppedJob and JobRegistry
    instances.
    '''
    path = tmpdir.join('test_stepped_job_in_registry').strpath

    reg = jobmgr.JobRegistry()

    job = jobmgr.SteppedJob(path, registry=reg)

    assert job in reg

    jobmgr.Step('fail', 'python', ['-c', 'print()'], job, data_regex='.*txt')

    with pytest.raises(RuntimeError):
        jobmgr.Step('fail', 'python', ['-c', 'print()'], job, data_regex='.*txt')


def test_stepped_job_run( tmpdir ):
    '''
    Test for the SteppedJob class. Completely run a job.
    '''
    path = tmpdir.join('test_stepped_job_run').strpath

    reg = jobmgr.JobRegistry()

    job = jobmgr.SteppedJob(path, registry=reg)

    executable = 'python'

    opts_create = [
        '-c',
        'with open("dummy.txt", "wt") as f: f.write("testing")'
        ]

    jobmgr.Step('create', executable, opts_create, job, data_regex='.*txt')

    opts_consume = [
        '-c',
        'import sys; f = open(sys.argv[1]); print(f.read())'
        ]

    jobmgr.Step('consume', executable, opts_consume, job, data_regex='.*txt')

    job.start()
    job.wait()
    job.steps.watchdog.stop()
    reg.watchdog.stop()

    assert job.status() == jobmgr.core.StatusCode.terminated


def test_stepped_job_steps( tmpdir ):
    '''
    Test for the SteppedJob class. If one step fails, it should kill the rest.
    '''
    path = tmpdir.join('test_stepped_job_steps').strpath

    reg = jobmgr.JobRegistry()

    job = jobmgr.SteppedJob(path, registry=reg)

    executable = 'python'

    opts_create = ['-c', 'cause error']
    jobmgr.Step('create', executable, opts_create, job, data_regex='.*txt')

    opts_consume = ['-c', 'print("should run fine")']
    jobmgr.Step('consume', executable, opts_consume, job, data_regex='.*txt')

    job.start()
    job.wait()
    job.steps.watchdog.stop()
    reg.watchdog.stop()

    assert all(map(lambda s: s.status() == jobmgr.core.StatusCode.killed,
                   job.steps))


def test_stepped_job_kill( tmpdir ):
    '''
    Test for the SteppedJob class. If one step is killed, it should kill the
    rest.
    '''
    path = tmpdir.join('test_stepped_job_kill').strpath

    reg = jobmgr.JobRegistry()

    job = jobmgr.SteppedJob(path, registry=reg)

    executable = 'python'

    opts_create = [
        '-c',
        'while True: pass; open("dummy.txt", "wt").write("testing")'
        ]
    jobmgr.Step('create', executable, opts_create, job, data_regex='.*txt')

    opts_consume = [
        '-c',
        'import sys; f = open(sys.argv[1]); print(f.read())'
        ]
    jobmgr.Step('consume', executable, opts_consume, job, data_regex='.*txt')

    job.start()
    job.kill()
    job.steps.watchdog.stop()
    reg.watchdog.stop()

    assert job.status() == jobmgr.core.StatusCode.killed
