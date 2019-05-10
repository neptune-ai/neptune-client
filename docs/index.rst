.. neptune-client-docs documentation master file, created by
   sphinx-quickstart on Thu Apr 18 15:45:30 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

What is Neptune?
================

`Neptune <https://neptune.ml/>`_ is a collaboration platform for data science / machine learning teams that focuses on three areas:

* **Track**: all metrics and outputs in your data science or machine learning project. It can be model training curves, visualizations, input data, calculated features and so on.
* **Organize**: automatically transform tracked data into a knowledge repository.
* **Collaborate**: share, compare and discuss your work across data science project.

What is Neptune client?
=======================

`Neptune client <https://github.com/neptune-ml/neptune-client>`_ is open-source, Python library that allows you to communicate with Neptune platform directly from your code.

.. note:: Make sure to `register to Neptune <https://neptune.ml/register>`_, to use it.

Installation
============

.. code:: bash

    pip install neptune-client

Once installed, ``import neptune`` in your code to use it.

What next?
==========

There are three major options:

#. **Tutorials** --> learn how to use Neptune client step-by-step.
#. **Package reference** --> Learn how to use Python API.
#. **Cheat-sheets** --> Take a look in case you need to quickly refresh some practical information.

.. toctree::
   :maxdepth: 1
   :caption: Tutorials

   Minimal example (5 minutes read) <tutorials/minimal>
   Larger example (15 minutes read) <tutorials/larger>

.. toctree::
   :maxdepth: 1
   :caption: Package reference

   Session <technical_reference/session>
   Project <technical_reference/project>
   Experiment <technical_reference/experiment>

.. toctree::
   :maxdepth: 1
   :caption: Cheat-sheets

   Cheat-sheet <technical_reference/cheatsheet>


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
