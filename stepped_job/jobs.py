'''
Definition of job-like classes.
'''

__author__  = ['Miguel Ramos Pernas']
__email__   = ['miguel.ramos.pernas@cern.ch']

# Python
import inspect
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
from .core import JobManager, JobRegistry, StatusCode


__all__ = ['JobBase', 'Job', 'Step', 'SteppedJob']


class JobBase(object):

    __str_attrs__ = {
        'status'      : 'status',
        'output path' : '_odir'
    }

    def __init__( self, path, kill_event = None, registry = None ):
        '''
        Base class to create a directory for a job, holding also an ID and a
        status code.

        :param path: path to the desired directory.
        :type path: str
        :param registry: instance to register the object. If "None", the \
        object will be registered in the main :class:`JobManager` instance.
        :type registry: JobRegistry or None

        :ivar jid: Job ID, determined by the subdirectories in the output path.
        '''
        self._odir = utils.create_dir(path)

        self._status = StatusCode.new

        # Store the associated "kill" event
        if kill_event is not None:
            self._kill_event = kill_event
        else:
            self._kill_event = threading.Event()

        # Event to determine if the thread terminated without errors
        self._terminated_event = threading.Event()

        # Register the object
        if registry is None:
            registry = JobManager()

        self.jid = registry.register(self)

    def __del__( self ):
        '''
        Kill running processes on deletion.
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
        maxl = max(map(len, self.__str_attrs__.keys()))

        out = []
        for k, v in sorted(self.__str_attrs__.items()):

            attr = getattr(self, v)

            if inspect.ismethod(attr):
                attr = attr()

            out.append(' {:<{}} = {}'.format(k, maxl, attr))

        return '\n'.join([' {}: ('.format(self.full_jid())] + out + [' )'])

    def full_jid( self ):
        '''
        Return the full job ID for this job.
        In this class it is just the index on the :class:`JobRegistry` object
        who owns it.

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

    def update_status( self ):
        '''
        Update the status of the job.

        .. warning::
           This method is reserved to be used by the class :class:`Watchdog`.
           Using it on your own might cause undefined behaviour.
        '''
        pass

    def status( self ):
        '''
        Return the status of this job.

        :returns: status of this job.
        :rtype: int
        '''
        return self._status

    def wait( self ):
        '''
        Wait till the job is done.
        '''
        pass


class Job(JobBase):

    __str_attrs__ = utils.merge_dicts(JobBase.__str_attrs__, {'command': 'command'})

    def __init__( self, executable, opts, odir, kill_event = None, registry = None ):
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
        :param registry: instance to register the object. If "None", the \
        object will be registered in the main :class:`JobManager` instance.
        :type registry: JobRegistry or None

        :ivar executable: Command to be executed.
        :ivar jid: Job ID, determined by the subdirectories in the output path.
        '''
        super(Job, self).__init__(odir, kill_event, registry)

        self._status = StatusCode.new

        # Build the command to execute
        self.command = [executable] + opts

        # To hold the task
        self._task = None

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
        self._run_process(kill_event)

        if not kill_event.is_set():
            terminated_event.set()

    def _run_process( self, kill_event, extra_opts = None ):
        '''
        Create and run the process associated to this job.
        '''
        extra_opts = extra_opts if extra_opts is not None else []

        # Create the working directory if it does not exist. If it does,
        # remove the elements inside it but DO NOT remove the directory itself,
        # since it may lead to conflicts between jobs.
        if os.path.exists(self._odir):
            logging.info('Removing all files in "{}"'.format(self._odir))

            for e in os.listdir(self._odir):

                fp = os.path.join(self._odir, e)

                if os.path.isdir(fp):
                    shutil.rmtree(fp)
                else:
                    os.remove(fp)
        else:
            os.makedirs(self._odir)

        # Initialize the process
        proc = subprocess.Popen(self.command + extra_opts,
                                cwd=self._odir,
                                stdout=open(os.path.join(self._odir, 'stdout'), 'wt'),
                                stderr=open(os.path.join(self._odir, 'stderr'), 'wt')
        )

        # Check whether the thread is asked to be killed
        while proc.poll() == None:
            if kill_event.is_set():

                # Kill the running process
                logging.getLogger(__name__).warning(
                    'Killing running process for job "{}"'.format(self.full_jid()))
                proc.kill()

                # Really needed, otherwise it might enter again in the loop
                proc.wait()

        # If the process failed, propagate the "kill" signal
        if proc.poll():

            logging.getLogger(__name__).error(
                'Job "{}" has failed; see output in {}'.format(self.full_jid(), self._odir))

            kill_event.set()

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

    def update_status( self ):
        '''
        Update the status of the job.

        .. warning::
           This method is reserved to be used by the class :class:`Watchdog`.
           Using it on your own might cause undefined behaviour.
        '''
        if self._terminated_event.is_set():

            self._status = StatusCode.terminated

        elif self._task is not None:
            if not self._task.is_alive():
                if self._kill_event.is_set():
                    self._status = StatusCode.killed

    def wait( self ):
        '''
        Wait till the task is done.
        '''
        if self._task is not None:
            self._task.join()


class Step(Job):

    __str_attrs__ = utils.merge_dicts(Job.__str_attrs__, {'data regex': 'data_regex'})

    def __init__( self, name, executable, opts, parent, data_regex = None, data_builder = None ):
        '''
        Represent a step on a generation process.

        :param name: name of the step. It must be unique within a job.
        :type name: str
        :param executable: application/version to run.
        :type executable: str
        :param opts: options to be passed to the executable.
        :type opts: list(str)
        :param parent: parent job.
        :type parent: SteppedJob
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

        :ivar executable: Command to be executed. Input data is added when the \
        process just before execution (once it is defined).
        :ivar name: Name of the step.
        :ivar data_builder: Function that modifies the input data to parse it \
        to the executable.
        :ivar jid: Job ID, determined by the subdirectories in the output path.
        '''
        # Set the previous step queue. Need to do this before the object
        # is registered
        if len(parent.steps):
            self._prev_queue = parent.steps[-1]._queue
        else:
            self._prev_queue = None

        super(Step, self).__init__(executable,
                                   opts,
                                   os.path.join(os.path.abspath(parent._odir), name),
                                   kill_event=parent._kill_event,
                                   registry=parent.steps)

        self.name = name

        # This is the queue associated to this step
        self._queue = queue.Queue()

        # Set the command to define how the data is parsed to the executable
        if data_builder is None:
            self.data_builder = lambda d, *args, **kwargs: ' '.join(d)
        else:
            self.data_builder = data_builder

        self.data_regex = data_regex

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

        if not kill_event.is_set():
            self._run_process(kill_event, extra_opts)

        if kill_event.is_set():
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

            dr = re.compile(self.data_regex)

            # Build and store the requested output files
            matches = filter(lambda s: s is not None,
                             map(dr.match, os.listdir(self._odir)))

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

    def full_jid( self ):
        '''
        Return the full job ID for this step.
        This is the addition of the index on the :class:`JobRegistry` object
        who owns it and its name.

        :returns: full name of this step.
        :rtype: str
        '''
        return os.path.join(str(self.jid), self.name)


class SteppedJob(JobBase):

    def __init__( self, path = None, registry = None ):
        '''
        Instance to handle different steps, linked together. This object
        creates a new directory under "path" with a job ID. This job ID
        is expected to be a number, and the next to the greatest in the
        directory will be associated to the job (0 if none is found).

        :param path: path to the desired directory.
        :type path: str
        :param registry: instance to register the object. If "None", the \
        object will be registered in the main :class:`JobManager` instance.
        :type registry: JobRegistry or None

        :ivar steps: Steps managed by this job.
        :ivar jid: Job ID, determined by the subdirectories in the output path.
        '''
        super(SteppedJob, self).__init__(path, registry=registry)

        self.steps = JobRegistry()

    def __del__( self ):
        '''
        Kill running processes on deletion.
        '''
        self.kill()

        for s in self.steps:
            s.wait()

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

        # The step is automatically registered, no need to append it to
        # the inner registry.
        step = Step(name, executable, opts, self,
                    data_regex=data_regex,
                    data_builder=data_builder)

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

    def update_status( self ):
        '''
        Update the status of the job.

        .. warning::
           This method is reserved to be used by the class :class:`Watchdog`.
           Using it on your own might cause undefined behaviour.
        '''
        if self._status not in (StatusCode.terminated, StatusCode.killed):

            if all(map(lambda t: t.status() == StatusCode.terminated, self.steps)):

                logging.getLogger(__name__).info('Job terminated')

                self._status = StatusCode.terminated

            elif any(map(lambda t: t.status() == StatusCode.killed, self.steps)):

                logging.getLogger(__name__).warning(
                    'Job {} has been killed'.format(self.jid))

                self._status = StatusCode.killed

    def wait( self ):
        '''
        Wait for the steps for completion.
        '''
        for s in self.steps:
            s.wait()
