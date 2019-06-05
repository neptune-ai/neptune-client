Installation
============
**Install neptune-client**

.. code-block:: bash

    pip install neptune-client

**Install** `psutil <https://psutil.readthedocs.io/en/latest/>`_ **to see hardware monitoring charts**

.. code-block:: bash

    pip install psutil

Create experiment
=================

Minimal
-------

.. code-block::

    import neptune

    neptune.init('shared/onboarding')
    neptune.create_experiment()
    neptune.stop()

Basic
-----

.. code-block::

    import neptune

    # initialize session with Neptune
    neptune.init('shared/onboarding')

    # create experiment (all parameters are optional)
    neptune.create_experiment(name='first-pytorch-ever',
                              params={'lr': 0.0005,
                                      'dropout': 0.2},
                              properties={'key1': 'value1',
                                          'key2': 17,
                                          'key3': 'other-value'},
                              description='write longer description here',
                              tags=['list-of', 'tags', 'goes-here', 'as-list-of-strings'],
                              upload_source_files=['training_with_pytorch.py'])

    neptune.stop()

``params`` and ``properties`` are standard Python dict.

Auto clean-up
-------------
Make use of the ``with`` statement to ensure that clean-up code is executed - no need to invoke ``neptune.stop()``.

.. code-block::

    import neptune

    neptune.init('shared/onboarding')

    with neptune.create_experiment() as npt_exp:
        for i in range(1, 117):
            npt_exp.send_metric('iteration', i)
            npt_exp.send_metric('loss', 1 / i ** 0.5)

Track your work
===============

.. code-block::

    # send metric (numeric value)
    neptune.send_metric('log_loss', 0.753)

    # send text
    neptune.send_text('some-channel-name', 'evaluation time: 00:14:54')

    # send image (PIL object)
    neptune.send_image('image-channel-name', PIL_image)

    # send image (pass path to filse)
    neptune.send_image('image-channel-name', 'path/to/image.png')

    # send arbitrary artifact
    neptune.send_artifact('path/to/arbitrary_data.torch')

Organize your work
==================

.. code-block::

    # append tag
    neptune.append_tag('new_tag')

    # remove tag
    neptune.remove_tag('remove_this_tag')

    # set property
    neptune.set_property('new_key', 'some_value')

    # remove property
    neptune.remove_property('remove_this_key')

    # get experiment properties
    with neptune.create_experiment() as npt_exp:
        exp_paramaters = npt_exp.get_parameters()
        print(exp_paramaters)
