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
from unittest.mock import Mock

from neptune.new.attributes import Integer, String
from neptune.new.internal import operation
from neptune.new.internal.backends.utils import (
    build_operation_url,
    ExecuteOperationsBatchingManager,
)
from neptune.new.internal.container_type import ContainerType


class TestNeptuneBackendMock(unittest.TestCase):
    def test_building_operation_url(self):
        urls = {
            build_operation_url(
                "https://app.stage.neptune.ai", "api/leaderboard/v1/attributes/download"
            ),
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
            build_operation_url(
                "app.stage.neptune.ai", "api/leaderboard/v1/attributes/download"
            ),
            build_operation_url(
                "app.stage.neptune.ai", "/api/leaderboard/v1/attributes/download"
            ),
            build_operation_url(
                "app.stage.neptune.ai/", "api/leaderboard/v1/attributes/download"
            ),
            build_operation_url(
                "app.stage.neptune.ai/", "/api/leaderboard/v1/attributes/download"
            ),
        }
        self.assertEqual(
            {"https://app.stage.neptune.ai/api/leaderboard/v1/attributes/download"},
            urls,
        )


class TestExecuteOperationsBatchingManager(unittest.TestCase):
    def test_copies_splits_batches_and_resolve(self):
        backend = Mock()
        manager = ExecuteOperationsBatchingManager()

        operations = [
            operation.AssignInt(["a"], 12),
            operation.AssignString(["b/c"], "test"),
            operation.CopyAttribute(
                ["a"], str(uuid.uuid4()), ContainerType.RUN, ["b"], Integer
            ),
            operation.AssignFloat(["q/d"], 44.12),
            operation.AssignInt(["pp"], 12),
            operation.CopyAttribute(
                ["q/d"], str(uuid.uuid4()), ContainerType.RUN, ["b"], String
            ),
        ]

        for op in operations:
            manager.append(op)

        batches = list(manager.iterate_resolved_batches(backend))
        expected_batches = [
            operations[0:2],
            (
                [
                    operation.AssignInt(
                        operations[2].path, backend.get_int_attribute.return_value.value
                    )
                ]
                + operations[3:5]
            ),
            [
                operation.AssignString(
                    operations[5].path, backend.get_string_attribute.return_value.value
                )
            ],
        ]
        self.assertEqual(expected_batches, batches)

    def test_no_copies_is_ok(self):
        backend = Mock()
        manager = ExecuteOperationsBatchingManager()

        operations = [
            operation.AssignInt(["a"], 12),
            operation.AssignString(["b/c"], "test"),
            operation.AssignFloat(["q/d"], 44.12),
            operation.AssignInt(["pp"], 12),
        ]

        for op in operations:
            manager.append(op)

        batches = list(manager.iterate_resolved_batches(backend))
        self.assertEqual([operations], batches)

    def test_no_ops_is_ok(self):
        backend = Mock()
        manager = ExecuteOperationsBatchingManager()

        batches = list(manager.iterate_resolved_batches(backend))
        self.assertEqual([], batches)
        backend.assert_not_called()

    def test_subsequent_copies_is_ok(self):
        backend = Mock()
        manager = ExecuteOperationsBatchingManager()

        operations = [
            operation.CopyAttribute(
                ["a"], str(uuid.uuid4()), ContainerType.RUN, ["b"], Integer
            ),
            operation.CopyAttribute(
                ["q/d"], str(uuid.uuid4()), ContainerType.RUN, ["b"], String
            ),
            operation.CopyAttribute(
                ["pp"], str(uuid.uuid4()), ContainerType.RUN, ["b"], Integer
            ),
        ]

        for op in operations:
            manager.append(op)

        batches = list(manager.iterate_resolved_batches(backend))
        expected_batches = [
            [
                operation.AssignInt(
                    operations[0].path, backend.get_int_attribute.return_value.value
                )
            ],
            [
                operation.AssignString(
                    operations[1].path, backend.get_string_attribute.return_value.value
                )
            ],
            [
                operation.AssignInt(
                    operations[2].path, backend.get_int_attribute.return_value.value
                )
            ],
        ]
        self.assertEqual(expected_batches, batches)
