'''
Test functions for the "core" module.
'''

__author__  = ['Miguel Ramos Pernas']
__email__   = ['miguel.ramos.pernas@cern.ch']

# Local
import jobmgr


def test_job_registry():
    '''
    Tests for the JobRegistry instance
    '''
    j0 = jobmgr.JobRegistry()

    assert j0 is j0

    j1 = jobmgr.JobRegistry()

    assert j0 is not j1


def test_context_manager():
    '''
    Tests for the ContextManager instance
    '''
    with jobmgr.core.ContextManager() as cmgr:

        # Test singleton behaviour
        assert cmgr is jobmgr.core.ContextManager()

        # Very important to test that the JobManager imported from the top
        # module and that from "core" are the same.
        assert jobmgr.ContextManager() is jobmgr.core.ContextManager()


def test_watchdog( tmpdir ):
    '''
    Test the behaviour of the Watchdog class.
    '''
    path = tmpdir.join('test_watchdog').strpath

    cmd = ['-c', 'import time; time.sleep(0.3)']

    reg = jobmgr.JobRegistry()

    j0 = jobmgr.Job('python', cmd, path, registry=reg)
    j1 = jobmgr.Job('python', cmd, path, registry=reg)
    j2 = jobmgr.Job('python', cmd, path, registry=reg)

    jobs = (j0, j1, j2)

    # Start jobs
    for j in jobs:
        j.start()

    # Wait for completion
    for j in jobs:
        j.wait()

    # This ensures the status of the jobs are updated before the assert
    reg.watchdog.stop()

    assert all(
        map(lambda j: j._status == jobmgr.StatusCode.terminated,jobs)
        )
