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
import os
import sys
import threading
import traceback

import pandas as pd
import six
from pandas.errors import EmptyDataError

from neptune.api_exceptions import ExperimentAlreadyFinished
from neptune.exceptions import FileNotFound, InvalidChannelValue, NoChannelValue, NoExperimentContext
from neptune.internal.channels.channels import ChannelValue, ChannelType
from neptune.internal.channels.channels_values_sender import ChannelsValuesSender
from neptune.internal.execution.execution_context import ExecutionContext
from neptune.internal.storage.storage_utils import upload_to_storage
from neptune.internal.utils.image import get_image_content
from neptune.utils import align_channels_on_x, is_float


class Experiment(object):
    """It contains all the information about a Neptune Experiment

    This class lets you extract experiment by, short experiment id, names of all the channels,
    system properties and other properties, parameters, numerical channel values,
    information about the hardware utilization during the experiment

    Args:
        client(`neptune.Client'): Client object
        leaderboard_entry(`neptune.model.LeaderboardEntry`): LeaderboardEntry object

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

    Todo:
        Column sorting
    """

    def __init__(self, client, _id, internal_id, project_full_id):
        self._client = client
        self._id = _id
        self._internal_id = internal_id
        self._project_full_id = project_full_id
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
        return self._client.get_experiment(self._internal_id).tags

    def append_tag(self, tag):
        self._client.update_tags(experiment=self,
                                 tags_to_add=[tag],
                                 tags_to_delete=[])

    def remove_tag(self, tag):
        self._client.update_tags(experiment=self,
                                 tags_to_add=[],
                                 tags_to_delete=[tag])

    def get_channels(self):
        """Retrieve all channel names along with their representations for this experiment.

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

            >>> experiment.get_channels()

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
        x, y = self._get_valid_x_y(x, y)

        if not is_float(y):
            raise InvalidChannelValue(expected_type='float', actual_type=type(y).__name__)

        value = ChannelValue(x, dict(numeric_value=y), timestamp)
        self._channels_values_sender.send(channel_name, ChannelType.NUMERIC.value, value)

    def send_text(self, channel_name, x, y=None, timestamp=None):
        x, y = self._get_valid_x_y(x, y)

        if not isinstance(y, six.string_types):
            raise InvalidChannelValue(expected_type='str', actual_type=type(y).__name__)

        value = ChannelValue(x, dict(text_value=y), timestamp)
        self._channels_values_sender.send(channel_name, ChannelType.TEXT.value, value)

    def send_image(self, channel_name, x, y=None, name=None, description=None, timestamp=None):
        x, y = self._get_valid_x_y(x, y)

        input_image = dict(
            name=name,
            description=description,
            data=base64.b64encode(get_image_content(y)).decode('utf-8')
        )

        value = ChannelValue(x, dict(image_value=input_image), timestamp)
        self._channels_values_sender.send(channel_name, ChannelType.IMAGE.value, value)

    def send_artifact(self, artifact):
        """
        Raises:
            `StorageLimitReached`: When storage limit in the project has been reached.
        """
        if not os.path.exists(artifact):
            raise FileNotFound(artifact)

        upload_to_storage(files_list=[(os.path.abspath(artifact), os.path.basename(artifact))],
                          upload_api_fun=self._client.upload_experiment_output,
                          upload_tar_api_fun=self._client.extract_experiment_output,
                          experiment=self)

    def send_graph(self, graph_id, value):
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
        properties = {p.key: p.value for p in self._client.get_experiment(self.internal_id).properties}
        properties[key] = value
        return self._client.update_experiment(
            experiment=self,
            properties=properties
        )

    def remove_property(self, key):
        properties = {p.key: p.value for p in self._client.get_experiment(self.internal_id).properties}
        del properties[key]
        return self._client.update_experiment(
            experiment=self,
            properties=properties
        )

    def get_hardware_utilization(self):
        """Retrieve RAM, CPU and GPU utilization throughout the experiment.

        The returned DataFrame contains 2 columns (x_*, y_*) for each of: RAM, CPU and each GPU.
        The x_ column contains the time (in milliseconds) from the experiment start,
        while the y_ column contains the value of the appropriate metric.

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
            upload_stdout=upload_stdout,
            upload_stderr=upload_stderr,
            send_hardware_metrics=send_hardware_metrics,
            run_monitoring_thread=run_monitoring_thread,
            handle_uncaught_exceptions=handle_uncaught_exceptions
        )

    def stop(self, exc_tb=None):

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
        return self._id == o._id and self._internal_id == o._internal_id and self._project_full_id == o._project_full_id

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

    def _get_channel(self, channel_name, channel_type):
        channel = self._find_channel(channel_name)
        if channel is None:
            channel = self._create_channel(channel_name, channel_type)
        return channel

    def _find_channel(self, channel_name):
        return self.get_channels().get(channel_name, None)

    def _create_channel(self, channel_name, channel_type):
        return self._client.create_channel(self, channel_name, channel_type)


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
