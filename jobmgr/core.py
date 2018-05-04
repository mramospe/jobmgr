'''
Module with the core functions and classes
'''

__author__  = ['Miguel Ramos Pernas']
__email__   = ['miguel.ramos.pernas@cern.ch']

# Python
import logging
import threading
import time

# For python2 and python3 compatibility
try:
    import Queue as queue
except:
    import queue

__all__ = ['ContextManager', 'JobRegistry', 'StatusCode', 'Watchdog']


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


class ContextManager(JobRegistry):

    __instance    = None
    __initialized = False

    def __init__( self ):
        '''
        Singleton to represent a context to manage the jobs.
        This object contains general jobs whose registries have not been
        specified explicitely.
        It can be used by two different ways:

        - Context-like:

        >>> with ContextManager() as jobs:
        ...     pass
        >>>

        - As a file:

        >>> jobs = ContextManager()
        ... 'your code goes here...'
        >>> jobs.close()

        Calling :func:`ContextManager.close` is extremely necessary to prevent
        a deadlock, since the monitoring thread would lock the session.
        '''
        if ContextManager.__initialized:
            return

        super(ContextManager, self).__init__()

        ContextManager.__initialized = True

    def __new__( cls ):
        '''
        Return the stored instance if it exists.

        :returns: generated or already existing instance.
        :rtype: cls
        '''
        if cls.__initialized is False:

            cls.__instance = super(ContextManager, cls).__new__(cls)

        return cls.__instance

    def __enter__( self ):
        '''
        Initialize the context.
        '''
        return self

    def __exit__( self, *excinfo ):
        '''
        Wait for completion of the jobs.
        If a KeyboardInterrupt is raised, it will kill the running jobs.
        '''
        if any(map(lambda j: j.status() == StatusCode.running, self)):

            logging.getLogger(__name__).info(
                'Running jobs detected; waiting for completion')

            try:
                for j in self:
                    j.wait()
            except KeyboardInterrupt:
                logging.getLogger(__name__).warning('Killing running jobs')

        self.close()

    def close( self ):
        '''
        Close the current context.
        This must be called any time the instance of this class is created.
        '''
        self.watchdog.stop()
        self.__del__()


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
        The objects are passed throguh the :func:`Watchdog.watch` method.
        To be thread-safe, one must ensure to do not delete the jobs before
        stopping the watchdog monitoring (through :func:`Watchdog.stop`).
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
