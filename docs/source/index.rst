.. jobmgr documentation master file, created by
   sphinx-quickstart on Fri Dec  8 18:24:26 2017.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Introduction
============

.. include:: ../../README.rst
   :start-after: inclusion-marker-do-not-remove

The API
=======

This package provides an API based on `IPython <https://ipython.org/>`_, which
can be accessed via

.. code-block:: bash

   job-mgr

This will bring us to the standard `IPython <https://ipython.org/>`_ session,
with the classes from this package being loaded on the main scope.

.. code-block:: ipython

   '''
   Welcome to "jobmgr" version 1.0.0
   Access the job manager via "jobs"

   Documentation is available at:
   https://mramospe.github.io/jobmgr/

   For issues, questions and contributions, please visit:
   https://github.com/mramospe/jobmgr
   '''

   In [1]:

The main object you might want to interact with is the :class:`ContextManager`
class.
This instance takes control of any job created in the execution whose registry
has not been explicitly specified.
It can be interpreted as a list of jobs.
Let's first see its length:

.. code-block:: ipython

   In [1]: len(jobs)
   Out[1]: 0

This is because for the moment we don't have registered jobs.


Functions and classes
=====================

Here you can find the documentation on the functions and classes
of the jobmgr package. The class inheritance diagram is also shown.

.. automodapi:: jobmgr
   :no-heading:

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
