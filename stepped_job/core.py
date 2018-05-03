'''
Module with the core functions and classes
'''

__author__  = ['Miguel Ramos Pernas']
__email__   = ['miguel.ramos.pernas@cern.ch']

# Python
import atexit
import logging
import threading

# For python2 and python3 compatibility
try:
    import Queue as queue
except:
    import queue

__all__ = ['JobManager', 'JobRegistry', 'manager', 'StatusCode']


class Singleton(object):

    __instance    = None
    __initialized = False

    def __init__( self, derived ):
        '''
        Base class to represent a singleton.
        If an attempt is made to create another object of this kind, the
        same reference will be returned.
        '''
        if derived.__initialized:
            return

        super(Singleton, self).__init__()

        derived.__initialized = True

    def __new__( cls ):
        '''
        Return the stored instance if it exists.

        :returns: generated or already existing instance.
        :rtype: cls
        '''
        if cls.__initialized is False:

            cls.__instance = super(Singleton, cls).__new__(cls)

        return cls.__instance


class JobRegistry(list):

    def __init__( self ):
        '''
        Represent a registry of jobs.
        This object does not own the jobs, in the sense that it will not bring
        kill signals on deletion.
        '''
        super(JobRegistry, self).__init__()

    def __del__( self ):
        '''
        Safely kill the jobs and wait for completion.
        '''
        # Kill the non-terminated jobs
        for j in filter(lambda j: j.status() != StatusCode.terminated, self):
            j.kill()

        # Wait till the jobs finish their processes
        for j in self:
            j.wait()

    def __repr__( self ):
        '''
        Representation as a string.

        :returns: this class as a string.
        :rtype: str
        '''
        return self.__str__()

    def __str__( self ):
        '''
        Representation as a string.

        :returns: this class as a string.
        :rtype: str
        '''
        return '\n'.join(map(str, self))

    def register( self, job ):
        '''
        Register the given job, returning its new job ID.

        :param job: input job.
        :type job: JobBase
        :returns: next available job ID.
        :rtype: int
        '''
        if len(self):
            jid = max(map(lambda s: int(s), range(len(self)))) + 1
        else:
            jid = 0

        self.append(job)

        return jid


class JobManager(Singleton, JobRegistry):

    def __init__( self ):
        '''
        Singleton to hold and manage jobs.
        '''
        super(JobManager, self).__init__(self.__class__)

    def __del__( self ):
        '''
        Delete the JobManager. Prevent from creating zombie jobs, waiting for
        completion if they are running, or killing them if the user causes a
        KeyboardInterrupt exception to be raised.
        '''
        if any(map(lambda j: j.status() == StatusCode.running, self)):

            logging.getLogger(__name__).info(
                'Running jobs detected; waiting for completion')

            try:
                for j in self:
                    j.wait()
            except KeyboardInterrupt:
                logging.getLogger(__name__).warning('Killing running jobs')

        super(JobManager, self).__del__()


def manager():
    '''
    Return the JobManager instance. This is equivalent to attempt to build \
    a new instance of :class:`JobManager`.

    :returns: the instance to manage jobs.
    :rtype: JobManager
    '''
    return JobManager()


class StatusCode(object):
    '''
    Hold the different possible status of jobs and steps.
    '''
    new = 'new'
    ''' The instance has just been created. '''

    running = 'running'
    ''' The instance is running. '''

    terminated = 'terminated'
    ''' The execution has ended. '''

    killed = 'killed'
    ''' The job/step has failed or has been killed. '''
