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
from typing import Optional

from mock import MagicMock
from neptune.new.internal.operation_processors.operation_processor import OperationProcessor

from neptune.new.internal.operation_processors.sync_operation_processor import SyncOperationProcessor

from neptune.new.internal.backends.neptune_backend_mock import NeptuneBackendMock

from neptune.new.run import Run


_now = time.time()


class TestAttributeBase(unittest.TestCase):

    @staticmethod
    def _create_run(processor: Optional[OperationProcessor] = None):
        backend = NeptuneBackendMock()
        exp = backend.create_run(uuid.uuid4())
        if processor is None:
            processor = SyncOperationProcessor(exp.uuid, backend)
        _run = Run(exp.uuid, backend, processor, MagicMock(), MagicMock(), MagicMock(), MagicMock())
        _run.sync()
        _run.start()
        return _run

    @staticmethod
    def _random_path():
        return ["some", "random", "path", str(uuid.uuid4())]

    @staticmethod
    def _random_wait():
        return bool(random.getrandbits(1))

    @staticmethod
    def _now():
        return _now
