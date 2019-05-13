Minimal example
===============

Below is the smallest possible example that follows the theme: *from zero to first Neptune experiment*.

Register
--------

Go here: https://neptune.ml/register *(registration is free of charge)*

Copy API token
--------------
API token allows you to authorize your access to Neptune. Where is it? Under your User menu (top right side of the screen, like on the image below):

.. image:: https://gist.githubusercontent.com/kamil-kaczmarek/b3b939797fb39752c45fdadfedba3ed9/raw/410d2db447ab852aca99f22c565f665b297c4a6f/token.png

.. warning:: Always keep your API token secret - it is like password to the application.

Assign your API token to the bash environment variable:

.. code:: bash

    export NEPTUNE_API_TOKEN='YOUR_API_TOKEN'

or append this line to your .bashrc file.

Install neptune-client
----------------------

`Neptune client <https://github.com/neptune-ml/neptune-client>`_ is open source Python library that allows Users to integrate their Python scripts with Neptune.

.. code:: bash

    pip install neptune-client

Run Python script
-----------------

In Neptune, there is special, public organization: `shared` with public project: `onboarding <https://ui.neptune.ml/shared/onboarding/experiments>`_. In `shared`, public (yet anonymous) user `Neptuner <https://ui.neptune.ml/o/shared/neptuner>`_ has *public API token* that you can use to run below script immediately and see results in Neptune.

.. note:: Remember - for real work (or real fun) always use your private API token (never share it).

Run script below as regular Python code (in terminal: ``python start.py``) and see your experiment at the top of `experiments view <https://ui.neptune.ml/o/shared/org/onboarding/experiments>`_.

.. code:: Python

    import neptune

    # pick project, provide API token
    neptune.init('shared/onboarding',
                 'eyJhcGlfYWRkcmVzcyI6Imh0dHBzOi8vdWkubmVwdHVuZS5tbCIsImFwaV9rZXkiOiJiNzA2YmM4Zi03NmY5LTRjMmUtOTM5ZC00YmEwMzZmOTMyZTQifQ==')

    # create experiment
    neptune.create_experiment()

    # send some metrics
    n = 117
    for i in range(1, n):
        neptune.send_metric('iteration', i)
        neptune.send_metric('loss', 1/i**0.5)

    neptune.stop()

Congrats! You just ran your first Neptune experiment and checked results online.

.. note:: What did you just learn? Few concepts:

    * how to run Neptune experiment
    * how to track it online
    * how to use basic Neptune client features, like *create_experiment()* and *send_metric()*

What next?
----------
Go to Larger-example to learn more about `Neptune client <https://github.com/neptune-ml/neptune-client>`_ and its capabilities.
