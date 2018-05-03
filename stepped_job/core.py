'''
Module with the core functions and classes
'''

__author__  = ['Miguel Ramos Pernas']
__email__   = ['miguel.ramos.pernas@cern.ch']

# Python
import logging

__all__ = ['JobManager', 'JobRegistry', 'manager', 'StatusCode']


class JobRegistry(list):

    def __init__( self ):
        '''
        Represent a registry of jobs.
        '''
        super(JobRegistry, self).__init__()

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


class JobManager(JobRegistry):

    __instance    = None
    __initialized = False

    def __init__( self ):
        '''
        Singleton to hold and manage jobs. If an attempt is made to create
        another object of this kind, the same reference will be returned.
        '''
        if JobManager.__initialized:
            return

        super(JobManager, self).__init__()

        JobManager.__initialized = True

    def __new__( cls ):
        '''
        This class is a singleton, so a check is done to see whether an
        instance of it exists before building it.

        :returns: instance of the class.
        :rtype: JobManager
        '''
        if JobManager.__instance is None:

            JobManager.__instance = super(JobManager, cls).__new__(cls)

        return JobManager.__instance

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
                for j in self:
                    logging.getLogger(__name__).warning('Killing running jobs')
                    j.kill()


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
