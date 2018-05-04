'''
Module with the core functions and classes
'''

__author__  = ['Miguel Ramos Pernas']
__email__   = ['miguel.ramos.pernas@cern.ch']

# Python
import atexit
import logging
import threading
import time

# For python2 and python3 compatibility
try:
    import Queue as queue
except:
    import queue

__all__ = ['JobManager', 'JobRegistry', 'manager', 'StatusCode', 'Watchdog']


class JobRegistry(list):

    def __init__( self ):
        '''
        Represent a registry of jobs.
        This object does not own the jobs, in the sense that it will not bring
        kill signals on deletion.
        '''
        super(JobRegistry, self).__init__()

        self.watchdog = Watchdog()

    def __del__( self ):
        '''
        Safely kill the jobs and wait for completion.
        '''
        self.watchdog.stop()

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

        self.watchdog.watch(job)

        return jid


class JobManager(JobRegistry):

    __instance    = None
    __initialized = False

    def __init__( self ):
        '''
        Singleton to hold and manage jobs.
        If an attempt is made to create another object of this kind, the
        same reference will be returned.
        '''
        if JobManager.__initialized:
            return

        super(JobManager, self).__init__()

        JobManager.__initialized = True

    def __new__( cls ):
        '''
        Return the stored instance if it exists.

        :returns: generated or already existing instance.
        :rtype: cls
        '''
        if cls.__initialized is False:

            cls.__instance = super(JobManager, cls).__new__(cls)

        return cls.__instance

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


class Watchdog(object):

    def __init__( self ):
        '''
        Object to iterate over a set of jobs and update its status.
        The objects are passed throguh the :method:`Watchdog.watch` method.
        To be thread-safe, one must ensure to do not delete the jobs before
        terminating the watchdog (through :method:`Watchdog.terminate`).
        '''
        super(Watchdog, self).__init__()

        self._stop_event = threading.Event()
        self._job_queue  = queue.Queue()
        self._task       = None

        self.start()

    def __del__( self ):
        '''
        Terminate watching the jobs and free the queue.
        '''
        self.stop()

        while not self._job_queue.empty():
            self._job_queue.get()

    def _update_status( self ):
        '''
        Update the status of the jobs in the queue.
        '''
        jlst = []
        while not self._job_queue.empty():
            j = self._job_queue.get()
            j.update_status()

            jlst.append(j)

        for j in jlst:
            self._job_queue.put(j)

    def _watchdog( self ):
        '''
        Main function to watch for the jobs.
        '''
        while not self._stop_event.is_set():

            self._update_status()

            # To reduce the CPU consumption
            time.sleep(0.1)

        # Do the last update before exiting
        self._update_status()

    def start( self ):
        '''
        Start monitoring the jobs.
        '''
        if self._task is None:
            self._task = threading.Thread(target=self._watchdog)
            self._task.daemon = True
            self._task.start()

    def stop( self ):
        '''
        Stop monitoring the jobs.

        .. note::
           This must be done before deleting the jobs, to prevent
           destroying jobs in the queue.
        '''
        if self._task is not None:
            self._stop_event.set()
            self._task.join()
            self._task = None

    def watch( self, job ):
        '''
        Start monitoring the given job.
        '''
        self._job_queue.put(job)
