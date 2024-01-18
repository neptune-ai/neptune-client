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
import unittest
import uuid
from typing import Optional
from unittest.mock import (
    Mock,
    patch,
)

import pytest

from neptune.attributes import (
    Integer,
    String,
)
from neptune.exceptions import FetchAttributeNotFoundException
from neptune.internal import operation
from neptune.internal.backends.neptune_backend import NeptuneBackend
from neptune.internal.backends.utils import (
    ExecuteOperationsBatchingManager,
    _check_if_tqdm_installed,
    build_operation_url,
    which_progress_bar,
)
from neptune.internal.container_type import ContainerType
from neptune.typing import ProgressBarCallback
from neptune.utils import (
    NullProgressBar,
    TqdmProgressBar,
)


class CustomProgressBar(ProgressBarCallback):
    def __enter__(self):
        ...

    def __exit__(self, exc_type, exc_val, exc_tb):
        ...

    def update(self, *, by: int, total: Optional[int] = None) -> None:
        pass


class TestNeptuneBackendMock(unittest.TestCase):
    def test_building_operation_url(self):
        urls = {
            build_operation_url("https://app.stage.neptune.ai", "api/leaderboard/v1/attributes/download"),
            build_operation_url(
                "https://app.stage.neptune.ai",
                "/api/leaderboard/v1/attributes/download",
            ),
            build_operation_url(
                "https://app.stage.neptune.ai/",
                "api/leaderboard/v1/attributes/download",
            ),
            build_operation_url(
                "https://app.stage.neptune.ai/",
                "/api/leaderboard/v1/attributes/download",
            ),
            build_operation_url("app.stage.neptune.ai", "api/leaderboard/v1/attributes/download"),
            build_operation_url("app.stage.neptune.ai", "/api/leaderboard/v1/attributes/download"),
            build_operation_url("app.stage.neptune.ai/", "api/leaderboard/v1/attributes/download"),
            build_operation_url("app.stage.neptune.ai/", "/api/leaderboard/v1/attributes/download"),
        }
        self.assertEqual(
            {"https://app.stage.neptune.ai/api/leaderboard/v1/attributes/download"},
            urls,
        )


class TestExecuteOperationsBatchingManager(unittest.TestCase):
    def test_cut_batch_on_copy(self):
        backend = Mock(spec=NeptuneBackend)
        manager = ExecuteOperationsBatchingManager(backend)

        operations = [
            operation.AssignInt(["a"], 12),
            operation.AssignString(["b/c"], "test"),
            operation.CopyAttribute(["a"], str(uuid.uuid4()), ContainerType.RUN, ["b"], Integer),
            operation.AssignFloat(["q/d"], 44.12),
            operation.AssignInt(["pp"], 12),
            operation.CopyAttribute(["q/d"], str(uuid.uuid4()), ContainerType.RUN, ["b"], String),
        ]

        batch = manager.get_batch(operations)
        self.assertEqual(operations[0:2], batch.operations)
        self.assertEqual([], batch.errors)
        self.assertEqual(0, batch.dropped_operations_count)

    def test_get_nonempty_batch_with_copy_first(self):
        backend = Mock(spec=NeptuneBackend)
        manager = ExecuteOperationsBatchingManager(backend)

        operations = [
            operation.CopyAttribute(["a"], str(uuid.uuid4()), ContainerType.RUN, ["b"], Integer),
            operation.AssignFloat(["q/d"], 44.12),
            operation.AssignInt(["pp"], 12),
            operation.CopyAttribute(["q/d"], str(uuid.uuid4()), ContainerType.RUN, ["b"], String),
        ]

        batch = manager.get_batch(operations)
        expected_batch = [
            operation.AssignInt(operations[0].path, backend.get_int_attribute.return_value.value)
        ] + operations[1:3]
        self.assertEqual(expected_batch, batch.operations)
        self.assertEqual([], batch.errors)
        self.assertEqual(0, batch.dropped_operations_count)

    def test_no_copies_is_ok(self):
        backend = Mock(spec=NeptuneBackend)
        manager = ExecuteOperationsBatchingManager(backend)

        operations = [
            operation.AssignInt(["a"], 12),
            operation.AssignString(["b/c"], "test"),
            operation.AssignFloat(["q/d"], 44.12),
            operation.AssignInt(["pp"], 12),
        ]

        batch = manager.get_batch(operations)
        self.assertEqual(operations, batch.operations)
        self.assertEqual([], batch.errors)
        self.assertEqual(0, batch.dropped_operations_count)

    def test_no_ops_is_ok(self):
        backend = Mock(spec=NeptuneBackend)
        manager = ExecuteOperationsBatchingManager(backend)

        batch = manager.get_batch([])
        self.assertEqual([], batch.operations)
        self.assertEqual([], batch.errors)
        self.assertEqual(0, batch.dropped_operations_count)

    def test_subsequent_copies_is_ok(self):
        backend = Mock(spec=NeptuneBackend)
        manager = ExecuteOperationsBatchingManager(backend)

        operations = [
            operation.CopyAttribute(["a"], str(uuid.uuid4()), ContainerType.RUN, ["b"], Integer),
            operation.CopyAttribute(["q/d"], str(uuid.uuid4()), ContainerType.RUN, ["b"], String),
            operation.CopyAttribute(["pp"], str(uuid.uuid4()), ContainerType.RUN, ["b"], Integer),
        ]

        batch = manager.get_batch(operations)
        expected_batch = [operation.AssignInt(operations[0].path, backend.get_int_attribute.return_value.value)]
        self.assertEqual(expected_batch, batch.operations)
        self.assertEqual([], batch.errors)
        self.assertEqual(0, batch.dropped_operations_count)

    def test_handle_failed_copy(self):
        backend = Mock(spec=NeptuneBackend)
        backend.get_int_attribute.side_effect = FetchAttributeNotFoundException("b")
        manager = ExecuteOperationsBatchingManager(backend)

        operations = [
            operation.CopyAttribute(["q/d"], str(uuid.uuid4()), ContainerType.RUN, ["b"], Integer),
            operation.AssignInt(["a"], 12),
            operation.AssignString(["b/c"], "test"),
            operation.AssignInt(["pp"], 12),
        ]

        batch = manager.get_batch(operations)
        # skipped erroneous CopyAttribute
        self.assertEqual(operations[1:], batch.operations)
        self.assertEqual([backend.get_int_attribute.side_effect], batch.errors)
        self.assertEqual(1, batch.dropped_operations_count)


@patch("neptune.internal.backends.utils._check_if_tqdm_installed")
def test_which_progress_bar(mock_tqdm_installed):
    mock_tqdm_installed.return_value = True

    assert which_progress_bar(None) == TqdmProgressBar
    assert which_progress_bar(True) == TqdmProgressBar
    assert which_progress_bar(False) == NullProgressBar
    assert which_progress_bar(CustomProgressBar) == CustomProgressBar

    mock_tqdm_installed.return_value = False
    assert which_progress_bar(None) == NullProgressBar
    assert which_progress_bar(True) == NullProgressBar
    assert which_progress_bar(False) == NullProgressBar
    assert which_progress_bar(CustomProgressBar) == CustomProgressBar

    assert mock_tqdm_installed.call_count == 4  # 2 x 'None' + 2 x 'True'

    with pytest.raises(TypeError):
        which_progress_bar(1)


@patch.dict("sys.modules", {"tqdm": None})
def test_check_if_tqdm_installed_not_installed():
    assert not _check_if_tqdm_installed()


@patch.dict("sys.modules", {"tqdm": {}})
def test_check_if_tqdm_installed_installed():
    assert _check_if_tqdm_installed()
