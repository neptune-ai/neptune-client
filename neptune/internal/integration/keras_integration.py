#
# Copyright (c) 2019, Neptune Labs Sp. z o.o.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from neptune.exceptions import LibraryNotInstalled, NeptuneException

_integrated_with_keras = False


def integrate_with_keras(experiment_getter):
    global _integrated_with_keras  # pylint:disable=global-statement

    if _integrated_with_keras:
        return

    _integrate_with_keras(experiment_getter)

    _integrated_with_keras = True


def _integrate_with_keras(experiment_getter):
    try:
        import keras
    except ImportError:
        raise LibraryNotInstalled('keras')

    from keras.callbacks import BaseLogger, Callback  # pylint:disable=import-error

    class NeptuneLogger(Callback):

        def __init__(self, experiment=None, experiment_getter=None):
            super(NeptuneLogger, self).__init__()

            def get_exp():
                return experiment

            self._experiment_holder = get_exp if experiment is None else experiment_getter

        def on_batch_end(self, batch, logs=None):  # pylint:disable=unused-argument

            if logs is None:
                return

            for metric, value in logs.items():

                try:
                    if metric in ('batch', 'size'):
                        continue
                    name = 'keras_on_batch_end_' + metric
                    self._experiment_holder().send_metric(channel_name=name,
                                                          x=value,
                                                          y=None)
                except NeptuneException:
                    pass

        def on_epoch_end(self, epoch, logs=None):  # pylint:disable=unused-argument

            if logs is None:
                return

            for metric, value in logs.items():
                try:
                    if metric in ('epoch', 'size'):
                        continue

                    name = 'keras_on_epoch_end_' + metric
                    self._experiment_holder().send_metric(channel_name=name,
                                                          x=value,
                                                          y=None)
                except NeptuneException:
                    pass

    class KerasAggregateCallback(Callback):

        def __init__(self, *callbacks):
            super(KerasAggregateCallback, self).__init__()
            self.callbacks = callbacks

        def set_params(self, params):
            for callback in self.callbacks:
                callback.params = params

        def set_model(self, model):
            for callback in self.callbacks:
                callback.model = model

        def on_epoch_begin(self, epoch, logs=None):
            for callback in self.callbacks:
                callback.on_epoch_begin(epoch, logs=logs)

        def on_batch_end(self, batch, logs=None):
            for callback in self.callbacks:
                callback.on_batch_end(batch, logs=logs)

        def on_epoch_end(self, epoch, logs=None):
            for callback in self.callbacks:
                callback.on_epoch_end(epoch, logs=logs)

    def monkey_patched_BaseLogger(*args, **kwargs):
        return KerasAggregateCallback(BaseLogger(*args, **kwargs),
                                      NeptuneLogger(experiment_getter=experiment_getter))

    keras.callbacks.BaseLogger = monkey_patched_BaseLogger
