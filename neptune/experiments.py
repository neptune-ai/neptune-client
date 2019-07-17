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
import base64
import logging
import os
import sys
import threading
import traceback

import pandas as pd
import six
from pandas.errors import EmptyDataError

from neptune.api_exceptions import ExperimentAlreadyFinished, ChannelDoesNotExist
from neptune.exceptions import FileNotFound, InvalidChannelValue, NoChannelValue, NoExperimentContext, NotADirectory
from neptune.internal.channels.channels import ChannelValue, ChannelType, ChannelNamespace
from neptune.internal.channels.channels_values_sender import ChannelsValuesSender
from neptune.internal.execution.execution_context import ExecutionContext
from neptune.internal.storage.storage_utils import upload_to_storage
from neptune.internal.utils.image import get_image_content
from neptune.utils import align_channels_on_x, is_float, is_nan_or_inf

_logger = logging.getLogger(__name__)

_nan_warning_sent = False


class Experiment(object):
    """It contains all the information about a Neptune Experiment

    This class lets you extract experiment by, short experiment id, names of all the channels,
    system properties and other properties, parameters, numerical channel values,
    information about the hardware utilization during the experiment

    Args:
        client(`neptune.Client`): Client object
        project(`neptune.Project`)
        _id(`str`)
        internal_id(`str`): UUID

    Examples:
        Instantiate a session.

        >>> from neptune.sessions import Session
        >>> session = Session()

        Fetch a project and a list of experiments.

        >>> project = session.get_projects('neptune-ml')['neptune-ml/Salt-Detection']
        >>> experiments = project.get_experiments(state=['aborted'], owner=['neyo'], min_running_time=100000)

        Get an experiment instance.

        >>> experiment = experiments[0]
        >>> experiment
        Experiment(SAL-1609)
    """

    def __init__(self, client, project, _id, internal_id):
        self._client = client
        self._project = project
        self._id = _id
        self._internal_id = internal_id
        self._channels_values_sender = ChannelsValuesSender(self)
        self._execution_context = ExecutionContext(client, self)

    @property
    def id(self):
        """ Experiment short id

        Examples:
            Instantiate a session.

            >>> from neptune.sessions import Session
            >>> session = Session()

            Fetch a project and a list of experiments.

            >>> project = session.get_projects('neptune-ml')['neptune-ml/Salt-Detection']
            >>> experiments = project.get_experiments(state=['aborted'], owner=['neyo'], min_running_time=100000)

            Get an experiment instance.

            >>> experiment = experiments[0]

            Get experiment short id.

            >>> experiment.id
            'SAL-1609'

        """
        return self._id

    @property
    def name(self):
        return self._client.get_experiment(self._internal_id).name

    @property
    def state(self):
        return self._client.get_experiment(self._internal_id).state

    @property
    def internal_id(self):
        return self._internal_id

    @property
    def limits(self):
        # TODO: get limit from server
        return {
            'channels': {
                'numeric': 1000,
                'text': 100,
                'image': 100
            }
        }

    def get_system_properties(self):
        """Retrieve system properties like owner, times of creation and completion, worker type, etc.

        Returns:
            dict: A dictionary mapping a property name to value.

        Examples:
            Instantiate a session.

            >>> from neptune.sessions import Session
            >>> session = Session()

            Fetch a project and a list of experiments.

            >>> project = session.get_projects('neptune-ml')['neptune-ml/Salt-Detection']
            >>> experiments = project.get_experiments(state=['aborted'], owner=['neyo'], min_running_time=100000)

            Get an experiment instance.

            >>> experiment = experiments[0]

            Get experiment system properties.

            >>> experiment.get_system_properties

        Note:
            The list of supported system properties may change over time.

        """
        experiment = self._client.get_experiment(self._internal_id)
        return {
            'id': experiment.shortId,
            'name': experiment.name,
            'created': experiment.timeOfCreation,
            'finished': experiment.timeOfCompletion,
            'running_time': experiment.runningTime,
            'owner': experiment.owner,
            'size': experiment.storageSize,
            'tags': experiment.tags,
            'notes': experiment.description
        }

    def get_tags(self):
        """Get tags associated with experiment.

        Returns:
            :obj:`list` of :obj:`str` with all tags for this experiment.

        Example:
            Assuming that `experiment` is an instance of :class:`~neptune.experiments.Experiment`.

            .. code:: python3

                experiment.get_tags()
        """
        return self._client.get_experiment(self._internal_id).tags

    def append_tag(self, tag, *tags):
        """Append tag(s) to the current experiment.

        Alias: :meth:`~neptune.experiments.Experiment.append_tags`.
        Only ``[a-zA-Z0-9]`` and ``-`` (dash) characters are allowed in tags.

        Args:
            tag (single :obj:`str` or multiple :obj:`str` or :obj:`list` of :obj:`str`):
                Tag(s) to add to the current experiment.

                    * If :obj:`str` is passed, singe tag is added.
                    * If multiple - comma separated - :obj:`str` are passed, all of them are added as tags.
                    * If :obj:`list` of :obj:`str` is passed, all elements of the :obj:`list` are added as tags.

        Examples:

            .. code:: python3

                neptune.append_tag('new-tag')  # single tag
                neptune.append_tag('first-tag', 'second-tag', 'third-tag')  # few str
                neptune.append_tag(['first-tag', 'second-tag', 'third-tag'])  # list of str

        """
        if isinstance(tag, list):
            tags_list = tag
        else:
            tags_list = [tag] + list(tags)
        self._client.update_tags(experiment=self,
                                 tags_to_add=tags_list,
                                 tags_to_delete=[])

    def append_tags(self, tag, *tags):
        """Append tag(s) to the current experiment.

        Alias for: :meth:`~neptune.experiments.Experiment.append_tag`
        """
        self.append_tag(tag, *tags)

    def remove_tag(self, tag):
        """Removes single tag from the experiment.

        Args:
            tag (:obj:`str`): Tag to be removed

        Example:
            Assuming that `experiment` is an instance of :class:`~neptune.experiments.Experiment`.

            .. code:: python3

                # assuming experiment has tags: `['tag-1', 'tag-2']`.
                experiment.remove_tag('tag-1')

        Note:
            Removing a tag that is not assigned to this experiment is silently ignored.
        """
        self._client.update_tags(experiment=self,
                                 tags_to_add=[],
                                 tags_to_delete=[tag])

    def get_channels(self):
        """Alias for :meth:`~neptune.experiments.Experiment.get_logs`
        """
        return self.get_logs()

    def get_logs(self):
        """Retrieve all log names along with their last values for this experiment.

        Returns:
            :obj:`dict` - A dictionary mapping a log names to the log's last value.

        Example:
            Assuming that `experiment` is an instance of :class:`~neptune.experiments.Experiment`.

            .. code:: python3

                exp_logs = experiment.get_logs()

        """
        experiment = self._client.get_experiment(self.internal_id)
        channels_last_values_by_name = dict((ch.channelName, ch) for ch in experiment.channelsLastValues)
        channels = dict()
        for ch in experiment.channels:
            last_value = channels_last_values_by_name.get(ch.name, None)
            if last_value is not None:
                ch.x = last_value.x
                ch.y = last_value.y
            elif ch.lastX is not None:
                ch.x = ch.lastX
                ch.y = None
            else:
                ch.x = None
                ch.y = None
            channels[ch.name] = ch
        return channels

    def get_system_channels(self):
        """Retrieve all system channel names along with their representations for this experiment.

        Returns:
            dict: A dictionary mapping a channel name to channel.

        Examples:
            Instantiate a session.

            >>> from neptune.sessions import Session
            >>> session = Session()

            Fetch a project and a list of experiments.

            >>> project = session.get_projects('neptune-ml')['neptune-ml/Salt-Detection']
            >>> experiments = project.get_experiments(state=['aborted'], owner=['neyo'], min_running_time=100000)

            Get an experiment instance.

            >>> experiment = experiments[0]

            Get experiment channels.

            >>> experiment.get_system_channels()

        """
        channels = self._client.get_system_channels(self)
        return dict((ch.name, ch) for ch in channels)

    def upload_source_files(self, source_files):
        """
        Raises:
            `StorageLimitReached`: When storage limit in the project has been reached.
        """
        files_list = []
        for source_file in source_files:
            if not os.path.exists(source_file):
                raise FileNotFound(source_file)
            files_list.append((os.path.abspath(source_file), source_file))

        upload_to_storage(files_list=files_list,
                          upload_api_fun=self._client.upload_experiment_source,
                          upload_tar_api_fun=self._client.extract_experiment_source,
                          experiment=self)

    def send_metric(self, channel_name, x, y=None, timestamp=None):
        """Log metrics (numeric values) in Neptune.

        Alias for :meth:`~neptune.experiments.Experiment.log_metric`
        """
        return self.log_metric(channel_name, x, y, timestamp)

    def log_metric(self, log_name, x, y=None, timestamp=None):
        """Log metrics (numeric values) in Neptune

        | If a log with provided ``log_name`` does not exist, it is created automatically.
        | If log exists (determined by ``log_name``), then new value is appended to it.
        | See :ref:`Limits<limits-top>` for information about API and storage usage upper bounds.

        Args:
            log_name (:obj:`str`): The name of log, i.e. `mse`, `loss`, `accuracy`.
            x (:obj:`double`): Depending, whether ``y`` parameter is passed:

                * ``y`` not passed: The value of the log (data-point).
                * ``y`` passed: Index of log entry being appended. Must be strictly increasing.

            y (:obj:`double`, optional, default is ``None``): The value of the log (data-point).
            timestamp (:obj:`time`, optional, default is ``None``):
                Timestamp to be associated with log entry. Must be Unix time.
                If ``None`` is passed, `time.time() <https://docs.python.org/3.6/library/time.html#time.time>`_
                (Python 3.6 example) is invoked to obtain timestamp.

        Example:
            Assuming that `experiment` is an instance of :class:`~neptune.experiments.Experiment` and
            'accuracy' log does not exists:

            .. code:: python3

                # Both calls below have the same effect

                # Common invocation, providing log name and value
                experiment.log_metric('accuracy', 0.5)
                experiment.log_metric('accuracy', 0.65)
                experiment.log_metric('accuracy', 0.8)

                # Providing both x and y params
                experiment.log_metric('accuracy', 0, 0.5)
                experiment.log_metric('accuracy', 1, 0.65)
                experiment.log_metric('accuracy', 2, 0.8)

        Note:
            For efficiency, logs are uploaded in batches via a queue.
            Hence, if you log a lot of data, you may experience slight delays in Neptune web application.
        """
        x, y = self._get_valid_x_y(x, y)

        if not is_float(y):
            raise InvalidChannelValue(expected_type='float', actual_type=type(y).__name__)

        if is_nan_or_inf(y):
            # pylint: disable=global-statement
            global _nan_warning_sent
            if not _nan_warning_sent:
                _nan_warning_sent = True
                _logger.warning(
                    'Invalid metric value: %s. Metrics with nan and inf values will not be sent to server',
                    y)
        else:
            value = ChannelValue(x, dict(numeric_value=y), timestamp)
            self._channels_values_sender.send(log_name, ChannelType.NUMERIC.value, value)

    def send_text(self, channel_name, x, y=None, timestamp=None):
        """Log text data in Neptune.

        Alias for :meth:`~neptune.experiments.Experiment.log_text`
        """
        return self.log_text(channel_name, x, y, timestamp)

    def log_text(self, log_name, x, y=None, timestamp=None):
        """Log text data in Neptune

        | If a log with provided ``log_name`` does not exist, it is created automatically.
        | If log exists (determined by ``log_name``), then new value is appended to it.
        | See :ref:`Limits<limits-top>` for information about API and storage usage upper bounds.

        Args:
            log_name (:obj:`str`): The name of log, i.e. `mse`, `my_text_data`, `timing_info`.
            x (:obj:`double` or :obj:`str`): Depending, whether ``y`` parameter is passed:

                * ``y`` not passed: The value of the log (data-point). Must be ``str``.
                * ``y`` passed: Index of log entry being appended. Must be strictly increasing.

            y (:obj:`str`, optional, default is ``None``): The value of the log (data-point).
            timestamp (:obj:`time`, optional, default is ``None``):
                Timestamp to be associated with log entry. Must be Unix time.
                If ``None`` is passed, `time.time() <https://docs.python.org/3.6/library/time.html#time.time>`_
                (Python 3.6 example) is invoked to obtain timestamp.

        Example:
            Assuming that `experiment` is an instance of :class:`~neptune.experiments.Experiment`:

            .. code:: python3

                # common case, where log name and data are passed
                neptune.log_text('my_text_data', str(data_item))

                # log_name, x and timestamp are passed
                neptune.log_text(log_name='logging_losses_as_text',
                                 x=str(val_loss),
                                 timestamp=1560430912)

        Note:
            For efficiency, logs are uploaded in batches via a queue.
            Hence, if you log a lot of data, you may experience slight delays in Neptune web application.
        """
        x, y = self._get_valid_x_y(x, y)

        if not isinstance(y, six.string_types):
            raise InvalidChannelValue(expected_type='str', actual_type=type(y).__name__)

        value = ChannelValue(x, dict(text_value=y), timestamp)
        self._channels_values_sender.send(log_name, ChannelType.TEXT.value, value)

    def send_image(self, channel_name, x, y=None, name=None, description=None, timestamp=None):
        """Log image data in Neptune.

        Alias for :meth:`~neptune.experiments.Experiment.log_image`
        """
        return self.log_image(channel_name, x, y, name, description, timestamp)

    def log_image(self, log_name, x, y=None, image_name=None, description=None, timestamp=None):
        """Log image data in Neptune

        | If a log with provided ``log_name`` does not exist, it is created automatically.
        | If log exists (determined by ``log_name``), then new value is appended to it.
        | See :ref:`Limits<limits-top>` for information about API and storage usage upper bounds.

        Args:
            log_name (:obj:`str`): The name of log, i.e. `mse`, `loss`, `accuracy`.
            x (:obj:`double` or :obj:`PIL image`): Depending, whether ``y`` parameter is passed:

                * ``y`` not passed: The value of the log (data-point). Must be :obj:`PIL image`.
                * ``y`` passed: Index of log entry being appended. Must be strictly increasing.

            y (:obj:`PIL image`, optional, default is ``None``): The value of the log (data-point).
            image_name (:obj:`str`, optional, default is ``None``): Image name
            description (:obj:`str`, optional, default is ``None``): Image description
            timestamp (:obj:`time`, optional, default is ``None``):
                Timestamp to be associated with log entry. Must be Unix time.
                If ``None`` is passed, `time.time() <https://docs.python.org/3.6/library/time.html#time.time>`_
                (Python 3.6 example) is invoked to obtain timestamp.

        Example:
            Assuming that `experiment` is an instance of :class:`~neptune.experiments.Experiment`:

            .. code:: python3

                # simple use
                experiment.log_image('bbox_images', PIL_object_1)
                experiment.log_image('bbox_images', PIL_object_2)
                experiment.log_image('bbox_images', PIL_object_3, image_name='difficult_case')

        Note:
            For efficiency, logs are uploaded in batches via a queue.
            Hence, if you log a lot of data, you may experience slight delays in Neptune web application.
        """
        x, y = self._get_valid_x_y(x, y)

        input_image = dict(
            name=image_name,
            description=description,
            data=base64.b64encode(get_image_content(y)).decode('utf-8')
        )

        value = ChannelValue(x, dict(image_value=input_image), timestamp)
        self._channels_values_sender.send(log_name, ChannelType.IMAGE.value, value)

    def send_artifact(self, artifact):
        """Save an artifact (file) in experiment storage.

        Alias for :meth:`~neptune.experiments.Experiment.log_artifact`
        """
        return self.log_artifact(artifact)

    def log_artifact(self, artifact):
        """Save an artifact (file) in experiment storage.

        Args:
            artifact (:obj:`str`): A path to the file in local filesystem.

        Raises:
            `StorageLimitReached`: When storage limit in the project has been reached.

        Example:
            Assuming that `experiment` is an instance of :class:`~neptune.experiments.Experiment`:

            .. code:: python3

                # simple use
                experiment.log_artifact('images/wrong_prediction_1.png')
        """
        if not os.path.exists(artifact):
            raise FileNotFound(artifact)

        upload_to_storage(files_list=[(os.path.abspath(artifact), os.path.basename(artifact))],
                          upload_api_fun=self._client.upload_experiment_output,
                          upload_tar_api_fun=self._client.extract_experiment_output,
                          experiment=self)

    def download_artifact(self, filename, destination_dir):
        path = "/{exp_id}/output/{file}".format(exp_id=self.id, file=filename)
        destination_path = "{dir}/{file}".format(dir=destination_dir, file=filename)

        if not os.path.exists(destination_dir):
            os.makedirs(destination_dir)
        elif not os.path.isdir(destination_dir):
            raise NotADirectory(destination_dir)

        self._client.download_data(self._project, path, destination_path)

    def send_graph(self, graph_id, value):
        """Alias for :meth:`~neptune.experiments.Experiment.log_graph`
        """
        return self.log_graph(graph_id, value)

    def log_graph(self, graph_id, value):
        """Upload a tensorflow graph for this experiment.

        Args:
            graph_id: a string UUID identifying the graph (managed by user)
            value: a string representation of Tensorflow graph

        Examples:
            Instantiate a session.

            >>> from neptune.sessions import Session
            >>> session = Session()

            Fetch a project and a list of experiments.

            >>> project = session.get_projects('neptune-ml')['neptune-ml/Salt-Detection']
            >>> experiments = project.get_experiments(state=['aborted'], owner=['neyo'], min_running_time=100000)

            Get an experiment instance.

            >>> experiment = experiments[0]

            Send graph to experiment.
            >>> import uuid
            >>> experiment.send_graph(str(uuid.uuid4()), str("tf.GraphDef instance"))

        """

        self._client.put_tensorflow_graph(self, graph_id, value)

    def reset_log(self, log_name):
        """Resets the log.

        Removes all data from the log and enables it to be reused from scratch.

        Args:
            log_name (:obj:`str`): The name of log to reset.

        Raises:
            `ChannelDoesNotExist`: When the log with name ``log_name`` does not exist on the server.

        Example:
            Assuming that `experiment` is an instance of :class:`~neptune.experiments.Experiment`.

            .. code:: python3

                experiment.reset_log('my_metric')

        Note:
            Check Neptune web application to see that reset charts have no data.

        """
        channel = self._find_channel(log_name, ChannelNamespace.USER)
        if channel is None:
            raise ChannelDoesNotExist(self.id, log_name)
        self._client.reset_channel(channel.id)

    def get_parameters(self):
        """Retrieve parameters for this experiment.

        Returns:
            dict: A dictionary mapping a parameter name to value.

        Examples:
            Instantiate a session.

            >>> from neptune.sessions import Session
            >>> session = Session()

            Fetch a project and a list of experiments.

            >>> project = session.get_projects('neptune-ml')['neptune-ml/Salt-Detection']
            >>> experiments = project.get_experiments(state=['aborted'], owner=['neyo'], min_running_time=100000)

            Get an experiment instance.

            >>> experiment = experiments[0]

            Get experiment parameters.

            >>> experiment.get_parameters()

        """
        experiment = self._client.get_experiment(self.internal_id)
        return dict((p.name, self._convert_parameter_value(p.value, p.parameterType)) for p in experiment.parameters)

    def get_properties(self):
        """Retrieve user-defined properties for this experiment.

        Returns:
            dict: A dictionary mapping a property key to value.

        Examples:
            Instantiate a session.

            >>> from neptune.sessions import Session
            >>> session = Session()

            Fetch a project and a list of experiments.

            >>> project = session.get_projects('neptune-ml')['neptune-ml/Salt-Detection']
            >>> experiments = project.get_experiments(state=['aborted'], owner=['neyo'], min_running_time=100000)

            Get an experiment instance.

            >>> experiment = experiments[0]

            Get experiment properties.

            >>> experiment.get_properties

        """
        experiment = self._client.get_experiment(self.internal_id)
        return dict((p.key, p.value) for p in experiment.properties)

    def set_property(self, key, value):
        """Set `key-value` pair as an experiment property.

        If property with given ``key`` does not exist, it adds a new one.

        Args:
            key (:obj:`str`): Property key.
            value (:obj:`obj`): New value of a property.

        Examples:
            Assuming that `experiment` is an instance of :class:`~neptune.experiments.Experiment`:

            .. code:: python3

                experiment.set_property('model', 'LightGBM')
                experiment.set_property('magic-number', 7)

        """
        properties = {p.key: p.value for p in self._client.get_experiment(self.internal_id).properties}
        properties[key] = value
        return self._client.update_experiment(
            experiment=self,
            properties=properties
        )

    def remove_property(self, key):
        """Removes a property with given key.

        Args:
            key (single :obj:`str`):
                Key of property to remove.

        Examples:
            Assuming that `experiment` is an instance of :class:`~neptune.experiments.Experiment`:

            .. code:: python3

                experiment.remove_property('host')

        """
        properties = {p.key: p.value for p in self._client.get_experiment(self.internal_id).properties}
        del properties[key]
        return self._client.update_experiment(
            experiment=self,
            properties=properties
        )

    def get_hardware_utilization(self):
        """Retrieve RAM, CPU and GPU utilization throughout the experiment.

        The returned DataFrame contains 2 columns (x_*, y_*) for each of: RAM, CPU and each GPU.
        The ``x_`` column contains the time (in milliseconds) from the experiment start,
        while the ``y_`` column contains the value of the appropriate metric.

        RAM and GPU memory usage is returned in gigabytes.
        CPU and GPU utilization is returned as a percentage (0-100).

        E.g. For an experiment using a single GPU, this method will return a DataFrame
        of the following columns:

        x_ram, y_ram, x_cpu, y_cpu, x_gpu_util_0, y_gpu_util_0, x_gpu_mem_0, y_gpu_mem_0

        The following values denote that after 3 seconds, the experiment used 16.7 GB of RAM.
        x_ram, y_ram = 3000, 16.7

        The returned DataFrame may contain NaNs if one of the metrics has more values than others.

        Returns:
            `pandas.DataFrame`: Dataframe containing the hardware utilization metrics throughout the experiment.

        Examples:
            Instantiate a session.

            >>> from neptune.sessions import Session
            >>> session = Session()

            Fetch a project and a list of experiments.

            >>> project = session.get_projects('neptune-ml')['neptune-ml/Salt-Detection']
            >>> experiments = project.get_experiments(state=['aborted'], owner=['neyo'], min_running_time=100000)

            Get an experiment instance.

            >>> experiment = experiments[0]

            Get hardware utilization channels.

            >>> experiment.get_hardware_utilization

        """
        metrics_csv = self._client.get_metrics_csv(self)
        try:
            return pd.read_csv(metrics_csv)
        except EmptyDataError:
            return pd.DataFrame()

    def get_numeric_channels_values(self, *channel_names):
        """
        Retrieve values of specified numeric channels.

        The returned DataFrame contains 1 additional column x along with the requested channels.

        E.g. get_numeric_channels_values('loss', 'auc') will return a DataFrame of the following structure:
            x, loss, auc

        The returned DataFrame may contain NaNs if one of the channels has more values than others.

        Args:
            *channel_names: variable length list of names of the channels to retrieve values for.

        Returns:
            `pandas.DataFrame`: Dataframe containing the values for the requested numerical channels.

        Examples:
            Instantiate a session.

            >>> from neptune.sessions import Session
            >>> session = Session()

            Fetch a project and a list of experiments.

            >>> project = session.get_projects('neptune-ml')['neptune-ml/Salt-Detection']
            >>> experiments = project.get_experiments(state=['aborted'], owner=['neyo'], min_running_time=100000)

            Get an experiment instance.

            >>> exp = experiments[0]

            Get numeric channel value for channels 'unet_0 batch sum loss' and 'unet_1 batch sum loss'.

            >>> batch_channels = exp.get_numeric_channels_values('unet_0 batch sum loss', 'unet_1 batch sum loss')
            >>> epoch_channels = exp.get_numeric_channels_values('unet_0 epoch_val sum loss', 'Learning Rate')

        Note:
            Remember to fetch the dataframe for the channels that have a common temporal/iteration axis x.
            For example combine epoch channels to one dataframe and batch channels to the other
        """

        channels_data = {}
        channels_by_name = self.get_channels()
        for channel_name in channel_names:
            channel_id = channels_by_name[channel_name].id
            try:
                channels_data[channel_name] = pd.read_csv(
                    self._client.get_channel_points_csv(self, channel_id),
                    header=None,
                    names=['x_{}'.format(channel_name), 'y_{}'.format(channel_name)],
                    dtype=float
                )
            except EmptyDataError:
                channels_data[channel_name] = pd.DataFrame(
                    columns=['x_{}'.format(channel_name), 'y_{}'.format(channel_name)],
                    dtype=float
                )

        return align_channels_on_x(pd.concat(channels_data.values(), axis=1, sort=False))

    def start(self,
              upload_source_files=None,
              abort_callback=None,
              logger=None,
              upload_stdout=True,
              upload_stderr=True,
              send_hardware_metrics=True,
              run_monitoring_thread=True,
              handle_uncaught_exceptions=True):

        if upload_source_files is None:
            main_file = sys.argv[0]
            main_abs_path = os.path.join(os.getcwd(), os.path.basename(main_file))
            if os.path.isfile(main_abs_path):
                upload_source_files = [os.path.relpath(main_abs_path, os.getcwd())]
            else:
                upload_source_files = []

        self.upload_source_files(upload_source_files)

        self._execution_context.start(
            abort_callback=abort_callback,
            logger=logger,
            upload_stdout=upload_stdout,
            upload_stderr=upload_stderr,
            send_hardware_metrics=send_hardware_metrics,
            run_monitoring_thread=run_monitoring_thread,
            handle_uncaught_exceptions=handle_uncaught_exceptions
        )

    def stop(self, exc_tb=None):
        """Marks experiment as finished (succeeded or failed).

        Args:
            exc_tb (:obj:`str`, optional, default is ``None``): Additional traceback information
                to be stored in experiment details in case of failure (stacktrace, etc).
                If this argument is ``None`` the experiment will be marked as succeeded.
                Otherwise, experiment will be marked as failed.

        Examples:
            Assuming that `experiment` is an instance of :class:`~neptune.experiments.Experiment`:

            .. code:: python3

                # Marks experiment as succeeded
                experiment.stop()

                # Assuming 'ex' is some exception,
                # it marks experiment as failed with exception info in experiment details.
                experiment.stop(str(ex))
        """

        self._channels_values_sender.join()

        try:
            if exc_tb is None:
                self._client.mark_succeeded(self)
            else:
                self._client.mark_failed(self, exc_tb)
        except ExperimentAlreadyFinished:
            pass

        self._execution_context.stop()

        pop_stopped_experiment()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_tb is None:
            self.stop()
        else:
            self.stop("\n".join(traceback.format_tb(exc_tb)) + "\n" + repr(exc_val))

    def __str__(self):
        return 'Experiment({})'.format(self.id)

    def __repr__(self):
        return str(self)

    def __eq__(self, o):
        # pylint: disable=protected-access
        return self._id == o._id and self._internal_id == o._internal_id and self._project == o._project

    def __ne__(self, o):
        return not self.__eq__(o)

    @staticmethod
    def _convert_parameter_value(value, parameter_type):
        if parameter_type == 'double':
            return float(value)
        else:
            return value

    @staticmethod
    def _get_valid_x_y(x, y):
        if x is None:
            raise NoChannelValue()

        if y is None:
            y = x
            x = None
        elif not is_float(x):
            raise InvalidChannelValue(expected_type='float', actual_type=type(x).__name__)

        return x, y

    def _send_channels_values(self, channels_with_values):
        self._client.send_channels_values(self, channels_with_values)

    def _get_channels(self, channels_names_with_types):
        existing_channels = self.get_channels()
        channels_by_name = {}
        for (channel_name, channel_type) in channels_names_with_types:
            channel = existing_channels.get(channel_name, None)
            if channel is None:
                channel = self._create_channel(channel_name, channel_type)
            channels_by_name[channel.name] = channel
        return channels_by_name

    def _get_channel(self, channel_name, channel_type, channel_namespace=ChannelNamespace.USER):
        channel = self._find_channel(channel_name, channel_namespace)
        if channel is None:
            channel = self._create_channel(channel_name, channel_type, channel_namespace)
        return channel

    def _find_channel(self, channel_name, channel_namespace):
        if channel_namespace == ChannelNamespace.USER:
            return self.get_channels().get(channel_name, None)
        elif channel_namespace == ChannelNamespace.SYSTEM:
            return self.get_system_channels().get(channel_name, None)
        else:
            raise RuntimeError("Unknown channel namesapce {}".format(channel_namespace))

    def _create_channel(self, channel_name, channel_type, channel_namespace=ChannelNamespace.USER):
        if channel_namespace == ChannelNamespace.USER:
            return self._client.create_channel(self, channel_name, channel_type)
        elif channel_namespace == ChannelNamespace.SYSTEM:
            return self._client.create_system_channel(self, channel_name, channel_type)
        else:
            raise RuntimeError("Unknown channel namesapce {}".format(channel_namespace))


_experiments_stack = []

__lock = threading.RLock()


def get_current_experiment():
    # pylint: disable=global-statement
    global _experiments_stack
    with __lock:
        if _experiments_stack:
            return _experiments_stack[len(_experiments_stack) - 1]
        else:
            raise NoExperimentContext()


def push_new_experiment(new_experiment):
    # pylint: disable=global-statement
    global _experiments_stack, __lock
    with __lock:
        _experiments_stack.append(new_experiment)
        return new_experiment


def pop_stopped_experiment():
    # pylint: disable=global-statement
    global _experiments_stack, __lock
    with __lock:
        if _experiments_stack:
            stopped_experiment = _experiments_stack.pop()
        else:
            stopped_experiment = None
        return stopped_experiment
