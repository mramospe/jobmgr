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


class Watchdog(object):

    def __init__( self ):

        super(Watchdog, self).__init__()

        self._stop_event      = threading.Event()
        self._terminate_event = threading.Event()
        self._job_queue       = queue.Queue()

        self._task = threading.Thread(target=self._watchdog,
                                      args=(self._terminate_event,
                                            self._stop_event,
                                            self._job_queue))
        self._task.daemon = True
        self._task.start()

    def __del__( self ):

        self._terminate_event.set()
        self._task.join()

        while not self._job_queue.empty():
            self._job_queue.get()

    def _watchdog( self, terminate_event, stop_event, job_queue ):
        '''
        '''
        while not terminate_event.is_set():

            # To reduce the CPU consumption
            time.sleep(0.1)

            if not stop_event.is_set():

                jlst = []
                while not job_queue.empty():
                    j = job_queue.get()
                    j.status()

                    jlst.append(j)

                for j in jlst:
                    job_queue.put(j)

    def start( self ):
        '''
        '''
        self._stop_event.unset()

    def stop( self ):
        self._stop_event.set()

    def terminate( self ):
        self._terminate_event.set()
        self._task.join()

    def watch( self, job ):
        self._job_queue.put(job)


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
        self.watchdog.terminate()

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
