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
import json
import unittest

from neptune.internal.operation import *

# pylint: disable=protected-access


class TestOperations(unittest.TestCase):

    def test_serialization_to_dict(self):
        classes = [cls.__name__ for cls in all_subclasses(Operation)]
        for obj in self._list_objects():
            classes.remove(obj.__class__.__name__)
            self.assertEqual(obj.__dict__, Operation.from_dict(json.loads(json.dumps(obj.to_dict()))).__dict__)
        self.assertEqual(classes, [])

    @staticmethod
    def _list_objects():
        return [
            AssignFloat(uuid.uuid4(), ["some", "random", "path", str(uuid.uuid4())], 5),
            AssignString(uuid.uuid4(), ["some", "random", "path", str(uuid.uuid4())], "a\rsdf\thr"),
            LogFloats(uuid.uuid4(), ["some", "random", "path", str(uuid.uuid4())], [
                LogSeriesValue[float](5, 4, 500),
                LogSeriesValue[float](3, None, 1000),
                LogSeriesValue[float](10, 10, 1234)
            ]),
            LogStrings(uuid.uuid4(), ["some", "random", "path", str(uuid.uuid4())], [
                LogSeriesValue[str]("jetybv", 1, 5),
                LogSeriesValue[str]("ghs\ner", 3, 123),
                LogSeriesValue[str]("r", None, 1356),
                LogSeriesValue[str]("ghsr", 13, 53682)
            ]),
            ClearFloatLog(uuid.uuid4(), ["some", "random", "path", str(uuid.uuid4())]),
            ClearStringLog(uuid.uuid4(), ["some", "random", "path", str(uuid.uuid4())]),
            AddStrings(uuid.uuid4(), ["some", "random", "path", str(uuid.uuid4())], {"asef", "asrge4"}),
            RemoveStrings(uuid.uuid4(), ["some", "random", "path", str(uuid.uuid4())], {"a\ne", "aeg\t4ger", "agrg"}),
            ClearStringSet(uuid.uuid4(), ["some", "random", "path", str(uuid.uuid4())]),
            DeleteVariable(uuid.uuid4(), ["some", "random", "path", str(uuid.uuid4())])
        ]
