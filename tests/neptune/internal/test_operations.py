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
import uuid

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
            AssignFloat(["some", "random", "path", str(uuid.uuid4())], 5),
            AssignString(["some", "random", "path", str(uuid.uuid4())], "a\rsdf\thr"),
            LogFloats(["some", "random", "path", str(uuid.uuid4())], [
                LogFloats.ValueType(5, 4, 500),
                LogFloats.ValueType(3, None, 1000),
                LogFloats.ValueType(10, 10, 1234)
            ]),
            LogStrings(["some", "random", "path", str(uuid.uuid4())], [
                LogStrings.ValueType("jetybv", 1, 5),
                LogStrings.ValueType("ghs\ner", 3, 123),
                LogStrings.ValueType("r", None, 1356),
                LogStrings.ValueType("ghsr", 13, 53682)
            ]),
            LogImages(["some", "random", "path", str(uuid.uuid4())], [
                LogImages.ValueType("base64_image_1", None, 2),
                LogImages.ValueType("base64_image_2", 0, 5),
            ]),
            ClearFloatLog(["some", "random", "path", str(uuid.uuid4())]),
            ClearStringLog(["some", "random", "path", str(uuid.uuid4())]),
            ClearImageLog(["some", "random", "path", str(uuid.uuid4())]),
            AddStrings(["some", "random", "path", str(uuid.uuid4())], {"asef", "asrge4"}),
            RemoveStrings(["some", "random", "path", str(uuid.uuid4())], {"a\ne", "aeg\t4ger", "agrg"}),
            ClearStringSet(["some", "random", "path", str(uuid.uuid4())]),
            DeleteVariable(["some", "random", "path", str(uuid.uuid4())])
        ]
