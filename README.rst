===========
stepped_job
===========

.. image:: https://img.shields.io/travis/mramospe/stepped_job.svg
   :target: https://travis-ci.org/mramospe/stepped_job

.. image:: https://img.shields.io/badge/documentation-link-blue.svg
   :target: https://mramospe.github.io/stepped_job/

.. inclusion-marker-do-not-remove

This package is aimed to provide a simple and friendly interface to launch jobs
with many steps in each of them. The steps are assumed to be related, so the
output data from the first is used in the second, successively.

Together with this package, an executable called "job_mgr" is also installed,
which allows to define a more friendly interface to handle the jobs (using
IPython). This is the prefered way of working, although one can use a simple
python session for this purpose.

Installation:
=============

To use this package, clone the repository and install with `pip`:

.. code-block:: bash

   git clone https://github.com/mramospe/stepped_job.git
   pip install stepped_job
