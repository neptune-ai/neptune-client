Cheatsheet
===========

Installation and setup
----------------------
**Install neptune-client**

.. code:: bash

    pip install neptune-client

**Install** `psutil <https://psutil.readthedocs.io/en/latest/>`_ **to see hardware monitoring charts**

.. code:: bash

    pip install psutil

Start experiment
----------------

.. code:: Python

    # import 'neptune-client' package
    import neptune

    # initialize session with Neptune
    neptune.init('USERNAME/PROJECT')

    # create experiment (all parameters are optional)
    neptune.create_experiment(name='first-pytorch-ever',
                              params={'lr': 0.0005,
                                      'dropout': 0.2},
                              properties={'key1': 'value1',
                                          'key2': 17,
                                          'key3': 'other-value'},
                              description='write longer description of what you are doing in this experiment',
                              tags=['list-of', 'tags', 'goes-here', 'as-list-of-strings']
                              )

    # stop experiment
    neptune.stop()

Start experiment (clean)
------------------------

.. code:: Python

    import neptune

    # create context, so you do not need to remember to close it at the end
    with neptune.create_experiment(name='experiment as context') as npt_exp:
        for i in range(1, 117):
            npt_exp.send_metric('iteration', i)
            npt_exp.send_metric('loss', 1 / i ** 0.5)

Track your work
---------------

.. code:: Python

    # send metric (numeric value)
    neptune.send_metric('log_loss', 0.753)

    # send text
    neptune.send_text('some-channel-name', 'evaluation time: 00:14:54')

    # send image
    neptune.send_image('image-channel-name', PIL_image)

    # send image (second way)
    neptune.send_image('image-channel-name', 'path/to/image.png')

    # send arbitrary artifact
    neptune.send_artifact('path/to/arbitrary_data.torch')

Organize your work
------------------

.. code:: Python

    # append tag
    neptune.append_tag('new_tag')

    # remove tag
    neptune.remove_tag('remove_this_tag')

    # set property
    neptune.set_property('new_key', 'some_value')

    # remove property
    neptune.remove_property('remove_this_key')
