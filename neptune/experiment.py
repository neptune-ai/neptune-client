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
import io
import os

from PIL import Image
import pandas as pd
from pandas.errors import EmptyDataError
import six

from neptune.utils import align_channels_on_x, is_float, map_values


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

        >>> from neptune.session import Session
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

    def __init__(self, client, leaderboard_entry):
        self._client = client
        self._leaderboard_entry = leaderboard_entry
        self._ping_thread = None
        self._hardware_metric_thread = None

    @property
    def id(self):
        """ Experiment short id

        Examples:
            Instantiate a session.

            >>> from neptune.session import Session
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
        return self._leaderboard_entry.id

    @property
    def internal_id(self):
        return self._leaderboard_entry.internal_id

    @property
    def system_properties(self):
        """Retrieve system properties like owner, times of creation and completion, worker type, etc.

        Returns:
            `pandas.DataFrame`: Dataframe that has 1 row containing a column for every property.

        Examples:
            Instantiate a session.

            >>> from neptune.session import Session
            >>> session = Session()

            Fetch a project and a list of experiments.

            >>> project = session.get_projects('neptune-ml')['neptune-ml/Salt-Detection']
            >>> experiments = project.get_experiments(state=['aborted'], owner=['neyo'], min_running_time=100000)

            Get an experiment instance.

            >>> experiment = experiments[0]

            Get experiment system properties.

            >>> experiment.system_properties

        Note:
            The list of supported system properties may change over time.

        """
        return self._simple_dict_to_dataframe(self._leaderboard_entry.system_properties)

    @property
    def channels(self):
        """Retrieve all channel names along with their types for this experiment.

        Returns:
            dict: A dictionary mapping a channel name to its type.

        Examples:
            Instantiate a session.

            >>> from neptune.session import Session
            >>> session = Session()

            Fetch a project and a list of experiments.

            >>> project = session.get_projects('neptune-ml')['neptune-ml/Salt-Detection']
            >>> experiments = project.get_experiments(state=['aborted'], owner=['neyo'], min_running_time=100000)

            Get an experiment instance.

            >>> experiment = experiments[0]

            Get experiment channels.

            >>> experiment.channels

        """
        return dict(
            (ch.name, ch.type) for ch in self._leaderboard_entry.channels
        )

    def send_metric(self, channel_name, x, y=None):
        x, y = self._get_valid_x_y(x, y)

        if not is_float(y):
            raise ValueError("Invalid value={} provided".format(y))

        self._send_channel_value(channel_name, 'numeric', x, dict(numeric_value=y))

    def send_text(self, name, x, y=None):
        x, y = self._get_valid_x_y(x, y)

        if isinstance(y, six.string_types):
            return ValueError("Invalid value={:100.100} provided".format(y))

        self._send_channel_value(name, 'text', x, dict(text_value=y))

    def send_image(self, channel_name, x, y=None, name=None, description=None):
        x, y = self._get_valid_x_y(x, y)

        input_image = dict(
            name=name,
            description=description,
            data=base64.b64encode(self._get_image_content(y)).decode('utf-8')
        )

        self._send_channel_value(channel_name, 'image', x, dict(image_value=input_image))

    @property
    def parameters(self):
        """Retrieve parameters for this experiment.

        Returns:
            `pandas.DataFrame`: Dataframe that has 1 row containing a column for every parameter.

        Examples:
            Instantiate a session.

            >>> from neptune.session import Session
            >>> session = Session()

            Fetch a project and a list of experiments.

            >>> project = session.get_projects('neptune-ml')['neptune-ml/Salt-Detection']
            >>> experiments = project.get_experiments(state=['aborted'], owner=['neyo'], min_running_time=100000)

            Get an experiment instance.

            >>> experiment = experiments[0]

            Get experiment parameters.

            >>> experiment.parameters

        """
        return self._simple_dict_to_dataframe(self._leaderboard_entry.parameters)

    @property
    def properties(self):
        """Retrieve user-defined properties for this experiment.

        Returns:
            `pandas.DataFrame`: Dataframe that has 1 row containing a column for every property.

        Examples:
            Instantiate a session.

            >>> from neptune.session import Session
            >>> session = Session()

            Fetch a project and a list of experiments.

            >>> project = session.get_projects('neptune-ml')['neptune-ml/Salt-Detection']
            >>> experiments = project.get_experiments(state=['aborted'], owner=['neyo'], min_running_time=100000)

            Get an experiment instance.

            >>> experiment = experiments[0]

            Get experiment properties.

            >>> experiment.properties

        """
        return self._simple_dict_to_dataframe(self._leaderboard_entry.properties)

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

            >>> from neptune.session import Session
            >>> session = Session()

            Fetch a project and a list of experiments.

            >>> project = session.get_projects('neptune-ml')['neptune-ml/Salt-Detection']
            >>> experiments = project.get_experiments(state=['aborted'], owner=['neyo'], min_running_time=100000)

            Get an experiment instance.

            >>> experiment = experiments[0]

            Get hardware utilization channels.

            >>> experiment.get_hardware_utilization

        """
        metrics_csv = self._client.get_metrics_csv(self._leaderboard_entry.internal_id)
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

            >>> from neptune.session import Session
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
        for channel_name in channel_names:
            channel_id = self._leaderboard_entry.channels_dict_by_name[channel_name].id
            try:
                channels_data[channel_name] = pd.read_csv(
                    self._client.get_channel_points_csv(self._leaderboard_entry.internal_id, channel_id),
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

    def stop(self, traceback=None):
        if traceback is None:
            self._client.mark_succeeded(self.internal_id)
        else:
            self._client.mark_failed(self.internal_id, traceback)

        if self._ping_thread:
            self._ping_thread.interrupt()
            self._ping_thread = None

        if self._hardware_metric_thread:
            self._hardware_metric_thread.interrupt()
            self._hardware_metric_thread = None

    def __str__(self):
        return 'Experiment({})'.format(self.id)

    def __repr__(self):
        return str(self)

    def __eq__(self, o):
        return self.__dict__ == o.__dict__

    def __ne__(self, o):
        return not self.__eq__(o)

    @staticmethod
    def _simple_dict_to_dataframe(d):
        return pd.DataFrame.from_dict(map_values(lambda x: [x], d))

    def _get_valid_x_y(self, x, y):
        if x is None:
            raise ValueError("No value provided")

        if y is None:
            y = x
            x = None
        elif not is_float(x):
            raise ValueError("Invalid value={} provided".format(x))

        return x, y

    def _send_channel_value(self, channel_name, channel_type, x, y):
        channel = self._get_channel(channel_name, channel_type)

        if x is None:
            if channel.x is None:
                channel.x = 0
            x = channel.x + 1
        elif x <= channel.x:
            raise ValueError("ValueError: X-coordinates must be strictly increasing. "
                             "Invalid Point({}, {:100.100}) for channel \"{}\"".format(x, y, channel_name))

        self._client.send_channel_value(self.internal_id, channel.id, x, y)

        channel.x = x
        channel.y = y

        return channel

    def _get_channel(self, channel_name, channel_type):
        channel = self._find_channel(channel_name)
        if channel is None:
            channel = self._create_channel(channel_name, channel_type)
        return channel

    def _find_channel(self, channel_name):
        return next((channel for channel in self._leaderboard_entry.channels if channel.name == channel_name), None)

    def _create_channel(self, channel_name, channel_type):
        channel = self._client.create_channel(self.internal_id, channel_name, channel_type)
        self._leaderboard_entry.add_channel(channel)
        return channel

    def _get_image_content(self, image):
        if isinstance(image, six.string_types):
            if not os.path.exists(image):
                raise ValueError("File {} doesn't exist".format(image))
            with open(image, 'r') as image_file:
                return image_file.read()

        elif isinstance(image, Image.Image):
            with io.BytesIO() as image_buffer:
                image.save(image_buffer, format='PNG')
                return image_buffer.getvalue()

        raise ValueError("Unsupported image value")
