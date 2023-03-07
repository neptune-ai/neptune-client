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

from datetime import datetime

from mock import (
    MagicMock,
    patch,
)

from neptune.attributes.atoms.datetime import (
    Datetime,
    DatetimeVal,
)
from neptune.internal.operation import AssignDatetime
from tests.unit.neptune.new.attributes.test_attribute_base import TestAttributeBase


class TestDatetime(TestAttributeBase):
    @patch("neptune.metadata_containers.metadata_container.get_operation_processor")
    def test_assign(self, get_operation_processor):
        now = datetime.now()
        value_and_expected = [
            (now, now.replace(microsecond=1000 * int(now.microsecond / 1000))),
            (
                DatetimeVal(now),
                now.replace(microsecond=1000 * int(now.microsecond / 1000)),
            ),
        ]

        for value, expected in value_and_expected:
            processor = MagicMock()
            get_operation_processor.return_value = processor
            path, wait = (
                self._random_path(),
                self._random_wait(),
            )
            with self._exp() as exp:
                var = Datetime(exp, path)
                var.assign(value, wait=wait)
                processor.enqueue_operation.assert_called_with(AssignDatetime(path, expected), wait=wait)

    def test_assign_type_error(self):
        values = [55, None]
        for value in values:
            with self.assertRaises(TypeError):
                Datetime(MagicMock(), MagicMock()).assign(value)

    def test_get(self):
        with self._exp() as exp:
            var = Datetime(exp, self._random_path())
            now = datetime.now()
            now = now.replace(microsecond=int(now.microsecond / 1000) * 1000)
            var.assign(now)
            self.assertEqual(now, var.fetch())
