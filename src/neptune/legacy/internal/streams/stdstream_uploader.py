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
import sys

from neptune.legacy.internal.channels.channels import ChannelNamespace
from neptune.legacy.internal.streams.channel_writer import ChannelWriter


class StdStreamWithUpload(object):
    def __init__(self, experiment, channel_name, stream):
        self._channel = experiment._get_channel(channel_name, "text", ChannelNamespace.SYSTEM)
        self._channel_writer = ChannelWriter(experiment, channel_name, ChannelNamespace.SYSTEM)
        self._stream = stream

    def write(self, data):
        self._stream.write(data)
        try:
            self._channel_writer.write(data)
        except:  # noqa: E722
            pass

    def isatty(self):
        return hasattr(self._stream, "isatty") and self._stream.isatty()

    def flush(self):
        self._stream.flush()

    def fileno(self):
        return self._stream.fileno()


class StdOutWithUpload(StdStreamWithUpload):
    def __init__(self, experiment):
        super(StdOutWithUpload, self).__init__(experiment, "stdout", sys.__stdout__)
        sys.stdout = self

    def close(self):
        sys.stdout = sys.__stdout__


class StdErrWithUpload(StdStreamWithUpload):
    def __init__(self, experiment):
        super(StdErrWithUpload, self).__init__(experiment, "stderr", sys.__stderr__)
        sys.stderr = self

    def close(self):
        sys.stderr = sys.__stderr__
