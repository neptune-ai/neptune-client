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

import neptune.alpha as neptune
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


class NeptuneMonitor(Callback):
    """Logs Keras metrics to Neptune.

    Goes over the `last_metrics` and `smooth_loss` after each batch and epoch
    and logs them to Neptune.

    If experiment parameter is not provided, Neptune will attempt to self-initialize using
    `project` and  `api_token` parameters

    See the example experiment here https://ui.neptune.ai/shared/keras-integration/e/KERAS-23/logs

    Args:
        experiment: `neptune.Experiment`, optional:
            Neptune experiment. If not provided, falls back on the current
            experiment.
        prefix: str, optional:
            Prefix that should be added before the `metric_name`
            and `valid_name` before logging to the appropriate channel.
            Default is empty string ('').
        project: str, optional:
            When experiment is `None`, create experiment in this project.
            Defaults to value of `NEPTUNE_PROJECT` environment variable.
        api_token: str, optional:
            When experiment is `None`, create experiment using this api_token.
            Defaults to value of `NEPTUNE_API_TOKEN` environment variable.

    Example:

        Initialize Neptune client:

        .. code:: python

            import neptune

            neptune.init(api_token='ANONYMOUS',
                         project_qualified_name='shared/keras-integration')

        Create Neptune experiment:

        .. code:: python

            neptune.create_experiment(name='keras-integration-example')

        Instantiate the monitor and pass
        it to callbacks argument of `model.fit()`:

        .. code:: python

            from neptunecontrib.monitoring.keras import NeptuneMonitor

            model.fit(x_train, y_train,
                      epochs=PARAMS['epoch_nr'],
                      batch_size=PARAMS['batch_size'],
                      callbacks=[NeptuneMonitor()])

    Note:
        You need to have Keras or Tensorflow 2 installed on your computer to use this module.
    """

    def __init__(self, experiment=None, prefix='', project=None, api_token=None):
        super().__init__()
        self._exp = experiment if experiment else neptune.init(project=project, api_token=api_token)
        self._prefix = prefix

    def _log_metrics(self, logs, trigger):
        if not logs:
            return

        prefix = self._prefix + trigger + '_'

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
