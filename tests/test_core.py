'''
Test functions for the "core" module.
'''

__author__  = ['Miguel Ramos Pernas']
__email__   = ['miguel.ramos.pernas@cern.ch']

# Local
import stepped_job


def test_job_registry():
    '''
    Tests for the JobRegistry instance
    '''
    j0 = stepped_job.JobRegistry()

    assert j0 is j0

    j1 = stepped_job.JobRegistry()

    assert j0 is not j1

def test_job_manager():
    '''
    Tests for the JobManager instance
    '''
    # Test singleton behaviour
    assert stepped_job.JobManager() is stepped_job.JobManager()

    # Very important to test that the JobManager imported from the top
    # module and that from "core" are the same.
    assert stepped_job.JobManager() is stepped_job.core.JobManager()


def test_manager():
    '''
    Test the function to get the job manager.
    '''
    # Test the function "manager"
    assert stepped_job.manager() is stepped_job.JobManager()
