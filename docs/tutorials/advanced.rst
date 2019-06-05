Advanced example
================
This example uses `Get Started with TensorFlow <https://www.tensorflow.org/tutorials#get-started-with-tensorflow>`_ as a base. It contains more features that neptune-client has to offer and put them in single script. Specifically, you will see several methods in action:

* ``send_text()``
* ``send_metric()``
* ``send_artifact()``
* ``append_tag()``
* ``send_text()``
* ``set_property()``

Copy it and save as *example.py*, then run it as usual: ``python example.py``. In this tutorial we make use of the public ``NEPTUNE_API_TOKEN`` of the public user `Neptuner <https://ui.neptune.ml/o/shared/neptuner>`_. Thus, when started you can see your experiment at the top of `experiments view <https://ui.neptune.ml/o/shared/org/onboarding/experiments>`_.

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
        .get_project('shared/onboarding')

    # create context with 'npt_exp', so you do not need to remember to close it at the end
    with project.create_experiment(name='neural-net-mnist',
                                   params=PARAMS,
                                   description='neural net trained on MNIST',
                                   upload_source_files=['example.py']) as npt_exp:

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
