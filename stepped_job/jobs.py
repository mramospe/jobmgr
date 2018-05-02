'''
Definition of job-like classes.
'''

__author__  = ['Miguel Ramos Pernas']
__email__   = ['miguel.ramos.pernas@cern.ch']

# Python
import logging
import os
import re
import subprocess
import shutil
import threading
from distutils.spawn import find_executable

# For python2 and python3 compatibility
try:
    import Queue as queue
except:
    import queue

# Local
from . import utils
from .core import JobManager, StatusCode


__all__ = ['JobBase', 'Job', 'Step', 'SteppedJob']


class JobBase(object):

    def __init__( self, path, kill_event = None, register = True ):
        '''
        Base class to create a directory for a job, holding also an ID and a
        status code.

        :param path: path to the desired directory.
        :type path: str
        :param register: whether to register this instance in the job manager.
        :type register: bool

        :ivar jid: Job ID, determined by the subdirectories in the output path.
        '''
        self._odir = utils.create_dir(path)

        self.jid = None

        mgr = JobManager()
        if mgr.keys():
            self.jid = max(map(lambda s: int(s), mgr.keys())) + 1
        else:
            self.jid = 0

        logging.getLogger(__name__).info(
            'Create new job with ID: {}'.format(self.jid))

        self._status = StatusCode.new

        # Store the associated "kill" event
        if kill_event is not None:
            self._kill_event = kill_event
        else:
            self._kill_event = threading.Event()

        # Event to determine if the thread terminated without errors
        self._terminated_event = threading.Event()

        # Register the object
        if register:
            mgr[self.jid] = self

    def __repr__( self ):
        '''
        Representation as a string.

        :returns: this class as a string.
        :rtype: str
        '''
        return self.__str__()

    def full_name( self ):
        '''
        Return the full name of this step.

        :returns: full name of this step.
        :rtype: str
        '''
        return str(self.jid)

    def kill( self ):
        '''
        Kill the job.
        '''
        self._kill_event.set()
        self.wait()

    def status( self ):
        '''
        Return the status of this job. This is the correct way of
        accessing the status of the job, since it updates it on
        request.
        This method is not implemented in this class.

        :returns: status of this job.
        :rtype: int
        :raises NotImplementedError: since it is an abstract method.
        '''
        raise NotImplementedError('Attempt to call abstract base class method')


class Job(JobBase):

    def __init__( self, executable, opts, odir, kill_event = None, register = True ):
        '''
        Represent a step on a generation process.

        :param name: name of the job.
        :type name: str
        :param executable: application/version to run.
        :type executable: str
        :param opts: options to be passed to the executable.
        :type opts: list(str)
        :param odir: where to create the output directory.
        :type odir: str
        :param kill_event: event associated to a possible "kill" signal. By \
        default an event is constructed.
        :type kill_event: threading.Event
        :param register: whether to register this instance in the job manager.
        :type register: bool

        :ivar executable: Command to be executed.
        :ivar jid: Job ID, determined by the subdirectories in the output path.
        '''
        super(Job, self).__init__(odir, kill_event, register)

        self._status = StatusCode.new

        # Build the command to execute
        self.command = [executable] + opts

        # To hold the task
        self._task = None

    def __str__( self ):
        '''
        Representation as a string.

        :returns: this class as a string.
        :rtype: str
        '''
        attrs = (self.full_name(), self.status(), self._odir)#, self._data_regex.pattern

        return '\n'.join([
                ' {}: (',
                '  status       = {}',
                '  output path  = {}',
#                '  output regex = {}',
                '  )'
                ]).format(*attrs)

    def _create_thread( self ):
        '''
        Create a new thread for this job.

        :returns: new thread for this job.
        :rtype: threading.Thread
        '''
        return threading.Thread(target=self._execute,
                                args=(self._kill_event,
                                      self._terminated_event,)
                                      )

    def _execute( self, kill_event, terminated_event ):
        '''
        Function to be sent to a new thread, and execute the step process.

        :param kill_event: event associated to a possible "kill" signal, to \
        be propagated among all the steps.
        :type kill_event: threading.Event
        :param terminated_event: event associated to the termination status \
        of the step.
        :type terminated_event: threading.Event
        '''
        killed = self._run_process(kill_event)

        if not killed:
            terminated_event.set()

    def _run_process( self, kill_event, extra_opts = None ):
        '''
        Create and run the process associated to this job.
        '''
        import logging

        extra_opts = extra_opts if extra_opts is not None else []

        # Create the working directory
        if os.path.exists(self._odir):
            shutil.rmtree(self._odir)

        os.makedirs(self._odir)

        # Initialize the process
        proc = subprocess.Popen(self.command + extra_opts,
                                cwd=self._odir,
                                stdout=open(os.path.join(self._odir, 'stdout'), 'wt'),
                                stderr=open(os.path.join(self._odir, 'stderr'), 'wt')
        )

        killed = False

        # Check whether the thread is asked to be killed
        while proc.poll() == None:
            if kill_event.is_set():

                # Kill the running process
                logging.getLogger(__name__).warning(
                    'Killing running process for job "{}"'.format(self.name))
                proc.kill()

                # Really needed, otherwise it might enter again in the loop
                proc.wait()

                # Flag as killed
                killed = True

        # If the process failed, propagate the "kill" signal
        if proc.poll():

            logging.getLogger(__name__).error(
                'Job "{}" has failed; see output in {}'.format(self.name, self._odir))

            killed = True

            kill_event.set()

        return killed

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

        path = os.path.join(self._odir, name)

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
        Create the associated task and start the job.
        '''
        self._status = StatusCode.running

        self._kill_event.clear()
        self._terminated_event.clear()

        self._task = self._create_thread()
        self._task.daemon = True
        self._task.start()

    def status( self ):
        '''
        Return the status of this job. This is the correct way of
        accessing the status of the job, since it updates it on
        request.

        :returns: status of this job.
        :rtype: int
        '''
        if self._terminated_event.is_set():

            self._status = StatusCode.terminated

        elif self._task is not None:
            if not self._task.is_alive():
                if self._kill_event.is_set():
                    self._status = StatusCode.killed

        return self._status

    def wait( self ):
        '''
        Wait till the task is done.
        '''
        self._task.join()


class Step(Job):

    def __init__( self, name, executable, opts, odir, kill_event, data_regex = None, data_builder = None, prev_step = None ):
        '''
        Represent a step on a generation process.

        :param name: name of the step. It must be unique within a job.
        :type name: str
        :param executable: application/version to run.
        :type executable: str
        :param opts: options to be passed to the executable.
        :type opts: list(str)
        :param odir: where to create the output directory.
        :type odir: str
        :param kill_event: event associated to a possible "kill" signal, to \
        be propagated among all the steps.
        :type kill_event: threading.Event
        :param data_regex: regex representing the output data to send to the \
        next step.
        :type data_regex: str
        :param data_builder: function to define the way how the data is passed \
        to the executable. It must take a list of strings (paths to the data \
        files), and return a merged string. The default behaviour is to \
        return list to be executed as: \
        <executable> file1 file2 ... \
        but one can also define the string so the argument is explicitely \
        specified, like: \
        <executable> --files file1 file2 ...
        :type data_builder: function
        :param prev_step: possible previous step.
        :type prev_step: Step

        :ivar executable: Command to be executed. Input data is added when the \
        process just before execution (once it is defined).
        :ivar name: Name of the step.
        :ivar data_builder: Function that modifies the input data to parse it \
        to the executable.
        :ivar jid: Job ID, determined by the subdirectories in the output path.
        '''
        super(Step, self).__init__(executable,
                                   opts,
                                   os.path.join(os.path.abspath(odir), name),
                                   kill_event=kill_event,
                                   register=False)

        self.name = name

        # Set the previous step queue
        if prev_step is not None:
            self._prev_queue = prev_step._queue
        else:
            self._prev_queue = None

        # This is the queue associated to this step
        self._queue = queue.Queue()

        # Set the command to define how the data is parsed to the executable
        if data_builder is None:
            self.data_builder = lambda d, *args, **kwargs: ' '.join(d)
        else:
            self.data_builder = data_builder

        self._data_regex = re.compile(data_regex)

    def __del__( self ):
        '''
        Kill the task and wait for completion.
        '''
        self._kill_event.set()
        self._task.join()

        while not self._queue.empty():
            self._queue.get()

    def _create_thread( self ):
        '''
        Create a new thread for this job.

        :returns: new thread for this job.
        :rtype: threading.Thread
        '''
        return threading.Thread(target=self._execute,
                                args=(self._prev_queue,
                                      self._queue,
                                      self._kill_event,
                                      self._terminated_event,)
                                      )

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

        if kill_event.is_set():

            killed = True

        else:

            killed = self._run_process(kill_event, extra_opts)

        if killed:
            # This message is displayed if this step is asked to be killed
            # or if the signal comes from other step. The "kill" signal is
            # propagated by including "None" as data to the next step.
            logging.getLogger(__name__).warning(
                'Step "{}" has been killed'.format(self.name))

            out_queue.put(None)
        else:
            # Notify the previous queue that we have finished
            if prev_queue is not None:
                prev_queue.task_done()

            # Build and store the requested output files
            matches = filter(lambda s: s is not None,
                             map(self._data_regex.match, os.listdir(self._odir)))

            output = [os.path.join(self._odir, m.string) for m in matches]

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

    def full_name( self ):
        '''
        Return the full name of this step.

        :returns: full name of this step.
        :rtype: str
        '''
        return os.path.join(str(self.jid), self.name)


class SteppedJob(JobBase):

    def __init__( self, path = None, register = True ):
        '''
        Instance to handle different steps, linked together. This object
        creates a new directory under "path" with a job ID. This job ID
        is expected to be a number, and the next to the greatest in the
        directory will be associated to the job (0 if none is found).

        :param path: path to the desired directory.
        :type path: str
        :param register: whether to register this instance in the job manager.
        :type register: bool

        :ivar steps: Steps managed by this job.
        :ivar jid: Job ID, determined by the subdirectories in the output path.
        '''
        super(SteppedJob, self).__init__(path, register=register)

        self.steps = []

    def __del__( self ):
        '''
        Kill the process when deleting the job.
        '''
        self.kill()

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
        :param data_regex: regex representing the output data to send to the \
        next step.
        :type data_regex: str
        :param data_builder: function to define the way how the data is passed \
        to the executable. It must take a list of strings (paths to the data \
        files), and return a merged string. The default behaviour is to \
        return list to be executed as: \
        <executable> file1 file2 ... \
        but one can also define the string so the argument is explicitely \
        specified, like: \
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
        if self.status() == StatusCode.running:
            logging.getLogger(__name__).warning(
                'Restarting unfinished job {}'.format(self.jid))
            self.kill()

        self._status = StatusCode.running

        self._kill_event.clear()
        self._terminated_event.clear()

        logging.getLogger(__name__).info(
            'Job {} with steps: {}'.format(self.jid, [s.name for s in self.steps]))

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

        logging.getLogger(__name__).info(
            'Starting job {} from step "{}"'.format(self.jid, self.steps[i].name))

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
        if self._status not in (StatusCode.terminated, StatusCode.killed):

            if all(map(lambda t: t.status() == StatusCode.terminated, self.steps)):

                logging.getLogger(__name__).info('Job terminated')

                self._status = StatusCode.terminated

            elif any(map(lambda t: t.status() == StatusCode.killed, self.steps)):

                logging.getLogger(__name__).warning(
                    'Job {} has been killed'.format(self.jid))

                self._status = StatusCode.killed

        return self._status

    def wait( self ):
        '''
        Wait for the steps for completion.
        '''
        for s in self.steps:
            s.wait()
