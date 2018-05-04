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
    path = tmpdir.join('test_job').strpath

    j0 = jobmgr.JobBase(path)

    assert j0 in jobmgr.JobManager()

    j1 = jobmgr.JobBase(path, registry=jobmgr.JobRegistry())

    assert j1 not in jobmgr.JobManager()


def test_job_in_registry( tmpdir ):
    '''
    Test the behaviour of the Job instance.
    '''
    path = tmpdir.join('test_job_in_registry').strpath

    # Test registered job
    j0 = jobmgr.Job('python', ['-c', 'print("testing")'], path)

    assert j0 in jobmgr.JobManager()

    # Test job with a different registry
    reg = jobmgr.JobRegistry()

    j1 = jobmgr.Job('python', ['-c', 'print("testing")'], path, registry=reg)

    assert j1 not in jobmgr.JobManager()
    assert j1 in reg


def test_job_completion( tmpdir ):
    '''
    Test the behaviour of the Job instance.
    '''
    path = tmpdir.join('test_job').strpath

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


def test_jobmgr_register( tmpdir ):
    '''
    Test for the SteppedJob class. Checks between SteppedJob and JobRegistry
    instances.
    '''
    path = tmpdir.join('test_jobmgr_in_registry').strpath

    reg = jobmgr.JobRegistry()

    job = jobmgr.SteppedJob(path, registry=reg)

    assert job in reg

    jobmgr.Step('fail', 'python', ['-c', 'print()'], job, data_regex='.*txt')

    with pytest.raises(RuntimeError):
        jobmgr.Step('fail', 'python', ['-c', 'print()'], job, data_regex='.*txt')


def test_jobmgr_run( tmpdir ):
    '''
    Test for the SteppedJob class. Completely run a job.
    '''
    path = tmpdir.join('test_job').strpath

    reg = jobmgr.JobRegistry()

    job = jobmgr.SteppedJob(path, registry=reg)

    executable = 'python'

    opts_create = [
        '-c',
        'with open("dummy.txt", "wt") as f: f.write("testing\\n")'
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


def test_jobmgr_steps( tmpdir ):
    '''
    Test for the SteppedJob class. If one step fails, it should kill the rest.
    '''
    path = tmpdir.join('test_jobmgr_steps').strpath

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


def test_jobmgr_kill( tmpdir ):
    '''
    Test for the SteppedJob class. If one step is killed, it should kill the
    rest.
    '''
    path = tmpdir.join('test_jobmgr_steps').strpath

    reg = jobmgr.JobRegistry()

    job = jobmgr.SteppedJob(path, registry=reg)

    executable = 'python'

    opts_create = [
        '-c',
        'open("dummy.txt", "wt").write("testing\\n"); while True: pass'
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
