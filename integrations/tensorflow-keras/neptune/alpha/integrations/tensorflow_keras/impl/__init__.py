#
# Copyright (c) 2021, Neptune Labs Sp. z o.o.
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
from typing import Optional

from neptune.alpha import Experiment
from neptune.alpha.exceptions import NeptuneException

# Note: we purposefully try to import `tensorflow.keras.callbacks.Callback`
# before `keras.callbacks.Callback` because the former is compatible with both
# `tensorflow.keras` and `keras`, while the latter is only compatible
# with `keras`. See https://github.com/keras-team/keras/issues/14125
try:
    from tensorflow.keras.callbacks import Callback
except ImportError:
    try:
        from keras.callbacks import Callback
    except ImportError:
        msg = """
        keras package not found. 

        As Keras is now part of Tensorflow you should install it by running
            pip install tensorflow"""
        raise ModuleNotFoundError(msg) # pylint:disable=undefined-variable


class NeptuneCallback(Callback):
    """Logs Keras metrics to Neptune.

    Goes over the `last_metrics` and `smooth_loss` after each batch and epoch
    and logs them to Neptune.

    If experiment parameter is not provided, Neptune will attempt to self-initialize using
    `project` and  `api_token` parameters

    See the example experiment here https://ui.neptune.ai/shared/keras-integration/e/KERAS-23/logs

    Args:
        experiment: `neptune.Experiment`:
            Neptune experiment, required.
        base_namespace: str, optional:
            Namespace, in which all series will be put.

    Example:

        Initialize Neptune client:

        .. code:: python

            import neptune

            experiment = neptune.init(api_token='ANONYMOUS',
                                      project='shared/keras-integration')

        Instantiate the monitor and pass
        it to callbacks argument of `model.fit()`:

        .. code:: python

            from neptunecontrib.monitoring.keras import NeptuneMonitor

            model.fit(x_train, y_train,
                      epochs=PARAMS['epoch_nr'],
                      batch_size=PARAMS['batch_size'],
                      callbacks=[NeptuneMonitor(experiment)])

    Note:
        You need to have Keras or Tensorflow 2 installed on your computer to use this module.
    """

    def __init__(self, experiment: Experiment, base_namespace: Optional[str] = None):
        super().__init__()
        if experiment is None or not isinstance(experiment, Experiment):
            raise ValueError("Neptune experiment is missing")
        self._exp = experiment
        self._namespace = ''
        if base_namespace:
            if not base_namespace.endswith("/"):
                self._namespace = base_namespace
            else:
                self._namespace = base_namespace + '/'

    def _log_metrics(self, logs, trigger):
        if not logs:
            return

        prefix = self._namespace + trigger + '/'

        for metric, value in logs.items():
            try:
                if metric in ('batch', 'size'):
                    continue
                name = prefix + metric
                self._exp[name].log(value)
            except NeptuneException:
                pass

    def on_batch_end(self, batch, logs=None):  # pylint:disable=unused-argument
        self._log_metrics(logs, 'batch')

    def on_epoch_end(self, epoch, logs=None):  # pylint:disable=unused-argument
        self._log_metrics(logs, 'epoch')
