'''
Module with the core functions and classes
'''

__author__  = ['Miguel Ramos Pernas']
__email__   = ['miguel.ramos.pernas@cern.ch']

# Python
import argparse, distutils, logging, os, re, subprocess, shutil, threading
from distutils.spawn import find_executable

# For python2 and python3 compatibility
try:
    import Queue as queue
except:
    import queue

# Local
from . import utils


__all__ = ['Job', 'JobManager', 'manager', 'status', 'Step']


class status:
    '''
    Hold the different possible status of jobs and steps.
    '''
    new        = 'new'
    running    = 'running'
    terminated = 'terminated'
    killed     = 'killed'


class Job:
    '''
    Represent an object to handle a job.
    '''
    def __init__( self, path = None ):
        '''
        Build the job, creating a new directory under "path" with a job ID. This
        job ID is expected to be a number, and the next to the greatest in the
        directory will be associated to the job (0 if none is found).

        :param path: path to the desired directory.
        :type path: str
        '''
        self._odir = utils.create_dir(path)
        self._kill_event = threading.Event()

        mgr = JobManager()
        if mgr.keys():
            self.jid = max(map(lambda s: int(s), mgr.keys())) + 1
        else:
            self.jid = 0

        logging.info('Create new job with ID: {}'.format(self.jid))

        self._status = status.new
        self.steps   = []

        # Register the object
        mgr[self.jid] = self

    def __del__( self ):
        '''
        Kill the process when deleting the job.
        '''
        self.kill()

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
        return 'Job {} with steps:\n'.format(self.jid) + '\n'.join(map(str, self.steps))

    def add_step( self, name, executable, opts, data_regex, data_builder = None ):
        '''
        Add a new step to the job, concatenating it with the previous one.

        :param name: name of the step.
        :type name: str
        :param executable: application/version to run.
        :type executable: str
        :param odir: where to create the output directory.
        :type odir: str
        :param data_regex: regex representing the output data to send to the next step.
        :type data_regex: str
        :param data_builder: function to define the way how the data is passed \
        to the executable. It must take a list of strings (paths to the data \
        files), and return a merged string. The default behaviour is to \
        return list to be executed as: \
        <executable> file1 file2 ... \
        but one can also define the string so the argument is explicitely \
        specified, like:
        <executable> --files file1 file2 ...
        :type data_builder: function
        :raises RuntimeError: if the name used for this step is already \
        being used in another step.
        '''
        if any(map(lambda s: s.name == name, self.steps)):
            raise RuntimeError('Unable to create step "{}"; another '\
                                   'with the same name already exists')

        if len(self.steps):
            prev_step = self.steps[-1]
        else:
            prev_step = None

        step = Step(name, executable, opts,
                    odir=self._odir,
                    data_regex=data_regex,
                    kill_event=self._kill_event,
                    data_builder=data_builder,
                    prev_step=prev_step)

        self.steps.append(step)

    def start( self, first = 0 ):
        '''
        Start the job from the given step ID.

        :param first: step ID to start processing.
        :type first: int or str
        '''
        if self.status() == status.running:
            logging.warning('Restarting unfinished job {}'.format(self.jid))
            self.kill()

        self._status = status.running

        logging.info('Job {} with steps: {}'.format(self.jid,
                                                    [s.name for s in self.steps]))

        # Clear any possible "kill" state
        self._kill_event.clear()

        if isinstance(first, str):

            error = True
            for i, s in enumerate(self.steps):
                if s.name == first:
                    error = False
                    break
            if error:
                raise LookupError('Unable to find step with name "{}"'.format(first))

        else:
            i = first

        logging.info('Starting job {} from step "{}"'.format(self.jid, self.steps[i].name))

        for s in reversed(self.steps[i + 1:]):
            s.clear_input_data()

        for s in self.steps[i:]:
            s.start()

    def status( self ):
        '''
        Check and return the status of the job. This is the correct way of
        accessing the status of the job, since it updates it on
        request

        :returns: status of the job.
        :rtype: str
        '''
        if self._status not in (status.terminated, status.killed):

            if all(map(lambda t: t.status() == status.terminated, self.steps)):

                logging.info('Job terminated')

                self._status = status.terminated

            elif any(map(lambda t: t.status() == status.killed, self.steps)):

                logging.warning('Job {} has been killed'.format(self.jid))

                self._status = status.killed

        return self._status

    def kill( self ):
        '''
        Kill the job, killing all the steps in it.
        '''
        self._kill_event.set()
        self.wait()

    def wait( self ):
        '''
        Wait for the steps for completion.
        '''
        for s in self.steps:
            s.wait()


class JobManager(dict):
    '''
    Singleton to hold and manage jobs.
    '''
    __instance = None

    def __init__( self ):
        '''
        Build the class as any other python dictionary.
        '''
        super(JobManager, self).__init__(self)

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
        return '\n'.join(self.values())


def manager():
    '''
    Return the JobManager instance.

    :returns: the instance to manage jobs.
    :rtype: JobManager
    '''
    return JobManager()


class Step:
    '''
    Represent a step on a generation process.
    '''
    def __init__( self, name, executable, opts, odir, kill_event, data_regex = None, data_builder = None, prev_step = None ):
        '''
        :param name: name of the step.
        :type name: str
        :param executable: application/version to run.
        :type executable: str
        :param opts: options to be passed to the executable.
        :type opts: list(str)
        :param odir: where to create the output directory.
        :type odir: str
        :param kill_event: event associated to a possible "kill" signal, to
        be propagated among all the steps.
        :type kill_event: threading.Event
        :param data_regex: regex representing the output data to send to the next step.
        :type data_regex: str

        :param data_builder: function to define the way how the data is passed \
        to the executable. It must take a list of strings (paths to the data \
        files), and return a merged string. The default behaviour is to \
        return list to be executed as: \
        <executable> file1 file2 ... \
        but one can also define the string so the argument is explicitely \
        specified, like:
        <executable> --files file1 file2 ...
        :type data_builder: function

        :param prev_step: possible previous step.
        :type prev_step: Step
        '''
        self.name = name

        self._status = status.new

        # Set the previous step queue
        if prev_step is not None:
            self._prev_queue = prev_step._queue
        else:
            self._prev_queue = None

        # Build the command to execute
        self.cmd = [executable] + opts
        logging.info('applying command' +  ' '.join(self.cmd))

        # This is the queue associated to this step
        self._queue = queue.Queue()

        # To hold the task
        self._task = None

        # Build the output directory
        self._opath = os.path.join(os.path.abspath(odir), name)

        # Set the command to define how the data is parsed to the executable
        if data_builder is None:
            self.data_builder = lambda d, *args, **kwargs: ' '.join(d)

        # Store the associated "kill" event
        self._kill_event = kill_event

        # Event to determine if the thread terminated without errors
        self._terminated_event = threading.Event()

        self._data_regex = re.compile(data_regex)

    def __del__( self ):
        '''
        Kill the task and wait for completion.
        '''
        self._kill_event.set()
        self._task.join()

        while not self._queue.empty():
            self._queue.get()

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
        attrs = (self.name, self.status(), self._opath, self._data_regex.pattern)

        return '\n'.join([
                ' {}: (',
                '  status       = {}',
                '  output path  = {}',
                '  output regex = {}',
                '  )'
                ]).format(*attrs)

    def _execute( self, prev_queue, out_queue, kill_event, terminated_event ):
        '''
        Function to be sent to a new thread, and execute the step process.

        :param kill_event: event associated to a possible "kill" signal, to \
        be propagated among all the steps.
        :type kill_event: threading.Event
        :param terminated_event: event associated to the termination status \
        of the step.
        :type terminated_event: threading.Event
        '''
        import logging

        if prev_queue is not None:
            # Get data from previous queue if it exists, and prepare input

            data = prev_queue.get()

            extra_opts = self.data_builder(data).split()
        else:
            # This is the first job, it has no input data

            data = None

            extra_opts = []

        killed = False

        if kill_event.is_set():

            # Flag to notify that the step has been killed
            killed = True

        else:
            # Create the working directory
            if os.path.exists(self._opath):
                shutil.rmtree(self._opath)

            os.makedirs(self._opath)

            # Initialize the process
            proc = subprocess.Popen(self.cmd + extra_opts,
                                    cwd=self._opath,
                                    stdout=open(os.path.join(self._opath, 'stdout'), 'wt'),
                                    stderr=open(os.path.join(self._opath, 'stderr'), 'wt')
                                    )

            # Check whether the thread is asked to be killed
            while proc.poll() == None:
                if kill_event.is_set():

                    # Kill the running process
                    logging.warning('Killing running process for step "{}"'.format(self.name))
                    proc.kill()

                    # Really needed, otherwise it might enter again in the loop
                    proc.wait()

                    # Flag as killed
                    killed = True

            # If the process failed, propagate the "kill" signal
            if proc.poll():

                logging.error('Step "{}" has failed; see output in {}'\
                              ''.format(self.name, self._opath))

                killed = True

                kill_event.set()

        if killed:
            # This message is displayed if this step is asked to be killed
            # or if the signal comes from other step. The "kill" signal is
            # propagated by including "None" as data to the next step.
            logging.warning('Step "{}" has been killed'.format(self.name))

            out_queue.put(None)
        else:
            # Notify the previous queue that we have finished
            if prev_queue is not None:
                prev_queue.task_done()

            # Build and store the requested output files
            matches = filter(lambda s: s is not None,
                             map(self._data_regex.match, os.listdir(self._opath)))

            output = [os.path.join(self._opath, m.string) for m in matches]

            out_queue.put(output)

            self._terminated_event.set()

        if prev_queue is not None:
            prev_queue.put(data)

    def clear_input_data( self ):
        '''
        Remove the input data from this step.
        '''
        if self._prev_queue is not None:
            if not self._prev_queue.empty():
                self._prev_queue.get()

    def peek( self, name = 'stdout', editor = None ):
        '''
        Open the "stdout" or "stderr" file in terminal mode.

        :param name: log-file to open ("stdout" or "stderr"). The default is \
        "stdout".
        :type name: str
        :param editor: editor to open. If None is provided, then it will \
        try to use "emacs" or "vi" to open the file. A RuntimeError is \
        raised if none of them exist in the system, or a ValueError if \
        the provided editor is not accesible.
        :raises ValueError: if name is not "stdout" or "stderr", or if \
        the provided editor is not accesible.
        :raises RuntimeError: if "editor = None" and neither "emacs" \
        nor "vi" are accesible.
        '''
        if name not in ('stdout', 'stderr'):
            raise ValueError('File to peek must be either "stdout" or "stderr"')

        path = os.path.join(self._opath, name)

        if editor is None:
            if find_executable('emacs'):
                editor = 'emacs -nw'
            elif find_executable('vi'):
                editor = 'vi'
            else:
                raise RuntimeError('Unable to find an apropiate text editor. '\
                                       'You can suggest one using the '\
                                       'argument "editor"')
        else:
            if not find_executable(editor):
                raise ValueError('Unable to find executable with name "{}"'\
                                     .format(editor))

        os.system('{} {}'.format(editor, path))

    def start( self ):
        '''
        Create the associated task and start the step.
        '''
        self._status = status.running

        self._terminated_event.clear()

        self._task = threading.Thread(target=self._execute,
                                      args=(self._prev_queue,
                                            self._queue,
                                            self._kill_event,
                                            self._terminated_event,)
                                      )
        self._task.daemon = True
        self._task.start()

    def status( self ):
        '''
        Returh the status of this step. This is the correct way of
        accessing the status of the step, since it updates it on
        request

        :returns: status of this step
        :rtype: int
        '''
        if self._terminated_event.is_set():

            self._status = status.terminated

        elif self._task is not None:
            if not self._task.is_alive():
                if self._kill_event.is_set():
                    self._status = status.killed

        return self._status

    def wait( self ):
        '''
        Wait till the task is done.
        '''
        self._task.join()
