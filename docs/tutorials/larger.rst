Larger example
==============


This example is equipped with more features and is presented on sightly more advanced example.

Before we start
---------------

* Install `psutil <https://psutil.readthedocs.io/en/latest/>`_ to see hardware monitoring charts (they are pretty useful)
* It this example we train simple `Keras <https://keras.io/>`_ model (with `TensorFlow <https://www.tensorflow.org/>`_ backend) on `MNIST dataset <http://yann.lecun.com/exdb/mnist/>`_. Install these libraries, instructions are here: https://www.tensorflow.org/install


Session
---------------
In the first tutorial, as you remember, we initialized Neptune using *neptune.init*:

.. code:: Python

    import neptune
    
    neptune.init('shared/onboarding',
                 'eyJhcGlfYWRkcmVzcyI6Imh0dHBzOi8vdWkubmVwdHVuZS5tbCIsImFwaV9rZXkiOiJiNzA2YmM4Zi03NmY5LTRjMmUtOTM5ZC00YmEwMzZmOTMyZTQifQ==')
                 
    neptune.create_experiment()

What we actually did, was to specify our *USER_NAME/PROJET_NAME* to **project_qualified_name** and *NEPTUNE_API_TOKEN* to **api_token** arguments.

Explicitly it would read:

.. code:: Python

    neptune.init(project_qualified_name='shared/onboarding',
                 api_token='eyJhcGlfYWRkcmVzcyI6Imh0dHBzOi8vdWkubmVwdHVuZS5tbCIsImFwaV9rZXkiOiJiNzA2YmM4Zi03NmY5LTRjMmUtOTM5ZC00YmEwMzZmOTMyZTQifQ==')
                 
.. note:: If you have your API token stored in the **NEPTUNE_API_TOKEN** environment variable you can leave the **api_token** argument empty.

That is not the only way of doing it but does make things simpler.
If you want to have more control you can explicitly start neptune session:

.. code:: Python

    from neptune.sessions import Session
    
    session = Session(api_token='eyJhcGlfYWRkcmVzcyI6Imh0dHBzOi8vdWkubmVwdHVuZS5tbCIsImFwaV9rZXkiOiJiNzA2YmM4Zi03NmY5LTRjMmUtOTM5ZC00YmEwMzZmOTMyZTQifQ==')

The session object lazily contains all of the projects that you have access too. 
You can fetch the project on which you want to work on by running:

.. code:: Python

    project = session.get_project(project_qualified_name='shared/onboarding')

And create a new experiment in that project. 

.. code:: Python

    experiment = project.create_experiment()

You can also programatically get other experiments from the project but it is a story for another day.
Read about it **here**.


Experiment
------------------

Let's dive into the **create_experiment** method and what you can track with it.
As you remember in the minimal example we started and experiment logged something to it and stopped it:

.. code:: Python

    neptune.create_experiment()
    neptune.send_metric('auc', 0.93)
    neptune.stop()
    
You can make it cleaner and create your experiments in **with statement** blocks:

.. code:: Python

    with neptune.create_experiment() as exp:
        exp.send_metric('auc', 0.93)

By doing that you will never forget to stop your experiments. We recommend you use this option.
Also, if you are creating more than one experiment, this approach keeps things civil. 

Ok, now that we know how to start and stop experiments let's see what happens in the app when you actually run it.

.. image:: ../_images/create_experiment_basic.gif

With every **create_experiment** a new record is added to Neptune with a state *running*. 
When you run **stop** on your experiment, either explicitly or implicitly, the state is changed to *succeeded*.



I will start with a complicated example and explain it step by step:



Sending logs
------------------

Advanced example
------------------

This example uses `Get Started with TensorFlow <https://www.tensorflow.org/tutorials#get-started-with-tensorflow>`_ as a base. Run it as regular Python code (in terminal: ``python example.py``) and see your experiment at the top of `experiments view <https://ui.neptune.ml/o/shared/org/onboarding/experiments>`_. Like in the previous tutorial, we use *API token* of the public (yet anonymous) user `Neptuner <https://ui.neptune.ml/o/shared/neptuner>`_.

.. code:: Python

    from hashlib import sha1

    import keras
    import neptune
    from keras import backend as K
    from keras.callbacks import Callback

    PARAMS = {'lr': 0.0001,
              'dropout': 0.2,
              'batch_size': 64,
              'optimizer': 'adam',
              'loss': 'sparse_categorical_crossentropy',
              'metrics': 'accuracy',
              'n_epochs': 5,
              }

    # prepare Keras callback to track training progress in Neptune
    class NeptuneMonitor(Callback):
        def __init__(self, neptune_experiment, n_batch):
            super().__init__()
            self.exp = neptune_experiment
            self.n = n_batch
            self.current_epoch = 0

        def on_batch_end(self, batch, logs=None):
            x = (self.current_epoch * self.n) + batch
            self.exp.send_metric(channel_name='batch end accuracy', x=x, y=logs['acc'])
            self.exp.send_metric(channel_name='batch end loss', x=x, y=logs['loss'])

        def on_epoch_end(self, epoch, logs=None):
            self.exp.send_metric('epoch end accuracy', logs['acc'])
            self.exp.send_metric('epoch end loss', logs['loss'])

            innovative_metric = logs['acc'] - 2 * logs['loss']
            self.exp.send_metric(channel_name='innovative_metric', x=epoch, y=innovative_metric)

            msg_acc = 'End of epoch {}, accuracy is {:.4f}'.format(epoch, logs['acc'])
            self.exp.send_text(channel_name='accuracy information', x=epoch, y=msg_acc)

            msg_loss = 'End of epoch {}, categorical crossentropy loss is {:.4f}'.format(epoch, logs['loss'])
            self.exp.send_text(channel_name='loss information', x=epoch, y=msg_loss)

            self.current_epoch += 1

    # retrieve project
    project = neptune.Session('eyJhcGlfYWRkcmVzcyI6Imh0dHBzOi8vdWkubmVwdHVuZS5tbCIsImFwaV9rZXkiOiJiNzA2YmM4Zi03NmY5LTRjMmUtOTM5ZC00YmEwMzZmOTMyZTQifQ==')\
        .get_project('shared/Tensor-Cell-Demo')

    # create context with 'npt_exp', so you do not need to remember to close it at the end
    with project.create_experiment(name='neural-net-mnist',
                                   params=PARAMS,
                                   description='neural net trained on MNIST',
                                   upload_source_files=['larger-example.py']) as npt_exp:

        # prepare data
        mnist = keras.datasets.mnist
        (x_train, y_train), (x_test, y_test) = mnist.load_data()
        x_train, x_test = x_train / 255.0, x_test / 255.0

        # calculate number of batches per epoch and track it in Neptune
        n_batches = x_train.shape[0] // npt_exp.get_parameters()['batch_size'] + 1
        npt_exp.set_property('n_batches', n_batches)

        # calculate train / test data hash and track it in Neptune
        train_sha = sha1(x_train).hexdigest()
        test_sha = sha1(x_test).hexdigest()
        npt_exp.send_text('train_version', train_sha)
        npt_exp.send_text('test_version', test_sha)

        # prepare model that use dropout parameter from Neptune
        model = keras.models.Sequential([
            keras.layers.Flatten(),
            keras.layers.Dense(512, activation=K.relu),
            keras.layers.Dropout(npt_exp.get_parameters()['dropout']),
            keras.layers.Dense(10, activation=K.softmax)
        ])

        # compile model using use parameters from Neptune
        model.compile(optimizer=npt_exp.get_parameters()['optimizer'],
                      loss=npt_exp.get_parameters()['loss'],
                      metrics=[npt_exp.get_parameters()['metrics']])

        # fit the model to data, using NeptuneMonitor callback
        model.fit(x_train, y_train,
                  epochs=PARAMS['n_epochs'],
                  batch_size=PARAMS['batch_size'],
                  callbacks=[NeptuneMonitor(npt_exp, n_batches)])

        # evaluate model on test data and track it in Neptune
        names = model.metrics_names
        values = model.evaluate(x_test, y_test)
        npt_exp.set_property(names[0], values[0])
        npt_exp.set_property(names[1], values[1])

        # save model in Neptune
        model.save_weights('model_weights.h5')
        npt_exp.send_artifact('model_weights.h5')
        npt_exp.append_tag('large lr')
        npt_exp.append_tag('compare')

Run this code and observe results `online <https://ui.neptune.ml/o/shared/org/onboarding/experiments>`_.

------------

What next?
----------

