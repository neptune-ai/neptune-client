#
# Copyright (c) 2020, Neptune Labs Sp. z o.o.
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
import random
import time
import unittest
import uuid
from contextlib import contextmanager
from unittest.mock import patch

from neptune import Run
from neptune.objects.neptune_object import NeptuneObject

_now = time.time()


class TestAttributeBase(unittest.TestCase):

    @staticmethod
    @contextmanager
    def _exp():
        with patch.object(
            NeptuneObject,
            "_create_object",
            lambda self: self._legacy_backend._create_container(self._custom_id, self.container_type, self._project_id),
        ):
            with Run(
                mode="disabled",
                capture_stderr=False,
                capture_traceback=False,
                capture_stdout=False,
                capture_hardware_metrics=False,
            ) as exp:
                yield exp

    @staticmethod
    def _random_path():
        return ["some", "random", "path", str(uuid.uuid4())]

    @staticmethod
    def _random_wait():
        return bool(random.getrandbits(1))

    @staticmethod
    def _now():
        return _now
