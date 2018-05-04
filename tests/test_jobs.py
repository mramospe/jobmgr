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


def test_job_in_registry( tmpdir ):
    '''
    Test the behaviour of the Job instance.
    '''
    path = tmpdir.join('test_job_in_registry').strpath

    # Test registered job
    j0 = stepped_job.Job('python', ['-c', 'print("testing")'], path)

    assert j0 in stepped_job.JobManager()

    # Test job with a different registry
    reg = stepped_job.JobRegistry()

    j1 = stepped_job.Job('python', ['-c', 'print("testing")'], path, registry=reg)

    assert j1 not in stepped_job.JobManager()
    assert j1 in reg


def test_job_completion( tmpdir ):
    '''
    Test the behaviour of the Job instance.
    '''
    path = tmpdir.join('test_job').strpath

    reg = stepped_job.JobRegistry()

    j0 = stepped_job.Job('python', ['-c', 'print("testing")'], path, registry=reg)
    j1 = stepped_job.Job('python', ['-c', 'while True: pass'], path, registry=reg)

    for j in (j0, j1):
        j.start()

    j0.wait()
    j1.kill()

    reg.watchdog.stop()

    assert j0.status() == stepped_job.StatusCode.terminated
    assert j1.status() == stepped_job.StatusCode.killed


def test_stepped_job_register( tmpdir ):
    '''
    Test for the SteppedJob class. Checks between SteppedJob and JobRegistry
    instances.
    '''
    path = tmpdir.join('test_stepped_job_in_registry').strpath

    reg = stepped_job.JobRegistry()

    job = stepped_job.SteppedJob(path, registry=reg)

    assert job in reg

    stepped_job.Step('fail', 'python', ['-c', 'print()'], job, data_regex='.*txt')

    with pytest.raises(RuntimeError):
        stepped_job.Step('fail', 'python', ['-c', 'print()'], job, data_regex='.*txt')


def test_stepped_job_run( tmpdir ):
    '''
    Test for the SteppedJob class. Completely run a job.
    '''
    path = tmpdir.join('test_job').strpath

    reg = stepped_job.JobRegistry()

    job = stepped_job.SteppedJob(path, registry=reg)

    executable = 'python'

    opts_create = [
        '-c',
        'with open("dummy.txt", "wt") as f: f.write("testing\\n")'
        ]

    stepped_job.Step('create', executable, opts_create, job, data_regex='.*txt')

    opts_consume = [
        '-c',
        'import sys; f = open(sys.argv[1]); print(f.read())'
        ]

    stepped_job.Step('consume', executable, opts_consume, job, data_regex='.*txt')

    job.start()
    job.wait()
    job.steps.watchdog.stop()
    reg.watchdog.stop()

    assert job.status() == stepped_job.core.StatusCode.terminated


def test_stepped_job_steps( tmpdir ):
    '''
    Test for the SteppedJob class. If one step fails, it should kill the rest.
    '''
    path = tmpdir.join('test_stepped_job_steps').strpath

    reg = stepped_job.JobRegistry()

    job = stepped_job.SteppedJob(path, registry=reg)

    executable = 'python'

    opts_create = ['-c', 'cause error']
    stepped_job.Step('create', executable, opts_create, job, data_regex='.*txt')

    opts_consume = ['-c', 'print("should run fine")']
    stepped_job.Step('consume', executable, opts_consume, job, data_regex='.*txt')

    job.start()
    job.wait()
    job.steps.watchdog.stop()
    reg.watchdog.stop()

    assert all(map(lambda s: s.status() == stepped_job.core.StatusCode.killed,
                   job.steps))


def test_stepped_job_kill( tmpdir ):
    '''
    Test for the SteppedJob class. If one step is killed, it should kill the
    rest.
    '''
    path = tmpdir.join('test_stepped_job_steps').strpath

    reg = stepped_job.JobRegistry()

    job = stepped_job.SteppedJob(path, registry=reg)

    executable = 'python'

    opts_create = [
        '-c',
        'open("dummy.txt", "wt").write("testing\\n"); while True: pass'
        ]
    stepped_job.Step('create', executable, opts_create, job, data_regex='.*txt')

    opts_consume = [
        '-c',
        'import sys; f = open(sys.argv[1]); print(f.read())'
        ]
    stepped_job.Step('consume', executable, opts_consume, job, data_regex='.*txt')

    job.start()
    job.kill()
    job.steps.watchdog.stop()
    reg.watchdog.stop()

    assert job.status() == stepped_job.core.StatusCode.killed
