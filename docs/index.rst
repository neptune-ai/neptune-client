What is Neptune?
================

`Neptune <https://neptune.ml/>`_ is a collaboration platform for data science / machine learning teams that focuses on three areas:

* **Track**: all metrics and outputs in your data science or machine learning project. It can be model training curves, visualizations, input data, calculated features and so on.
* **Organize**: automatically transform tracked data into a knowledge repository.
* **Collaborate**: share, compare and discuss your work across data science project.

What is Neptune client?
-----------------------
`Neptune client <https://github.com/neptune-ml/neptune-client>`_ is open source Python library that allows Users to integrate their Python scripts with Neptune.

.. note:: Make sure to `register to Neptune <https://neptune.ml/register>`_, to use it.

Installation
------------

.. code:: bash

    pip install neptune-client

Once installed, ``import neptune`` in your code to use it.

Example
-------

.. code-block::

   import neptune

   neptune.init('shared/onboarding')
   with neptune.create_experiment(name='simple_example'):
       neptune.append_tag('minimal-example')
       n = 117
       for i in range(1, n):
           neptune.send_metric('iteration', i)
           neptune.send_metric('loss', 1/i**0.5)
       neptune.set_property('n_iterations', n)

Example above creates Neptune `experiment <https://ui.neptune.ml/o/shared/org/onboarding/e/ON-26/charts>`_ in the project: *shared/onboarding* and logs *iteration* and *loss* metrics to Neptune in real time. It also presents common use case for Neptune client, that is tracking progress of machine learning experiments.


.. toctree::
   :maxdepth: 1
   :caption: Tutorials

   Get started (5 minutes read) <tutorials/get-started.rst>
   Session and Experiment <tutorials/session-and-experiment.rst>
   Advanced example <tutorials/advanced.rst>

.. toctree::
   :maxdepth: 1
   :caption: API reference

   Neptune <technical_reference/neptune.rst>
   Session <technical_reference/session.rst>
   Project <technical_reference/project.rst>
   Experiment <technical_reference/experiment.rst>
   Notebook <technical_reference/notebook.rst>
   Utils <technical_reference/utils.rst>

.. toctree::
   :maxdepth: 1
   :caption: Cheat-sheets

   Cheat-sheet <technical_reference/cheatsheet.rst>

.. toctree::
   :maxdepth: 1
   :caption: Miscellaneous

   API usage limits <limits.rst>

Bugs, feature requests and questions
------------------------------------

If you find yourself in any trouble drop an issue on `GitHub issues <https://github.com/neptune-ml/neptune-client/issues>`_,
fire a feature request on `GitHub feature request <https://github.com/neptune-ml/neptune-client/issues>`_
or ask us on the `Neptune community forum <https://community.neptune.ml/>`_
or `Neptune community spectrum <https://spectrum.chat/neptune-community>`_.

Indices
-------
* :ref:`genindex`
* :ref:`modindex`
