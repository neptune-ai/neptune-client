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

# pylint: disable=protected-access

from neptune.new.exceptions import MetadataInconsistency
from neptune.new.internal.backends.operations_preprocessor import OperationsPreprocessor
from neptune.new.internal.operation import AssignFloat, DeleteAttribute, AssignString, LogFloats, LogStrings, \
    LogImages, ClearFloatLog, ClearImageLog, AddStrings, RemoveStrings, ClearStringSet, ConfigFloatSeries, UploadFileSet

from tests.neptune.new.attributes.test_attribute_base import TestAttributeBase

FLog = LogFloats.ValueType
SLog = LogStrings.ValueType
ILog = LogImages.ValueType


class TestOperationsPreprocessor(TestAttributeBase):

    def test_delete_attribute(self):
        # given
        processor = OperationsPreprocessor()

        # when
        processor.process([
            DeleteAttribute(["a"]),
            AssignFloat(["a"], 1),
            DeleteAttribute(["a"]),

            AssignString(["b"], "2"),
            DeleteAttribute(["b"]),

            AssignString(["c"], "2"),
            AssignString(["c"], "3"),
            DeleteAttribute(["c"]),

            DeleteAttribute(["d"]),
            DeleteAttribute(["d"]),

            DeleteAttribute(["e"]),
        ])

        # then
        self.assertEqual(processor.get_operations(), [
            DeleteAttribute(["a"]),

            AssignString(["b"], "2"),
            DeleteAttribute(["b"]),

            AssignString(["c"], "3"),
            DeleteAttribute(["c"]),

            DeleteAttribute(["d"]),

            DeleteAttribute(["e"]),
        ])
        self.assertEqual(processor.get_errors(), [])

    def test_assign(self):
        # given
        processor = OperationsPreprocessor()

        # when
        processor.process([
            AssignFloat(["a"], 1),
            DeleteAttribute(["a"]),
            AssignString(["a"], "111"),

            DeleteAttribute(["b"]),
            AssignFloat(["b"], 2),

            AssignFloat(["c"], 3),
            AssignString(["c"], "333"),

            AssignString(["d"], "44"),

            AssignFloat(["e"], 5),
            AssignFloat(["e"], 10),
            AssignFloat(["e"], 33),
        ])

        # then
        self.assertEqual(processor.get_operations(), [
            AssignFloat(["a"], 1),
            DeleteAttribute(["a"]),
            AssignString(["a"], "111"),

            DeleteAttribute(["b"]),
            AssignFloat(["b"], 2),

            AssignFloat(["c"], 3),

            AssignString(["d"], "44"),

            AssignFloat(["e"], 33),
        ])
        self.assertEqual(processor.get_errors(), [
            MetadataInconsistency("Cannot perform AssignString operation on c: Attribute is not a String")
        ])

    def test_series(self):
        # given
        processor = OperationsPreprocessor()

        # when
        processor.process([
            LogFloats(["a"], [FLog(1, 2, 3)]),
            ConfigFloatSeries(["a"], min=7, max=70, unit="%"),
            DeleteAttribute(["a"]),
            LogStrings(["a"], [SLog("111", 3, 4)]),

            DeleteAttribute(["b"]),
            LogStrings(["b"], [SLog("222", None, 6)]),

            LogFloats(["c"], [FLog(1, 2, 3)]),
            LogFloats(["c"], [FLog(10, 20, 30), FLog(100, 200, 300)]),

            LogStrings(["d"], [SLog("4", 111, 222)]),

            ClearFloatLog(["e"]),

            LogImages(["f"], [ILog("1", 2, 3)]),
            LogImages(["f"], [ILog("10", 20, 30), FLog("100", 200, 300)]),
            LogImages(["f"], [ILog("1", 2, 3)]),
            LogImages(["f"], [ILog("10", 20, 30), FLog("100", 200, 300)]),
            ClearImageLog(["f"]),
            LogImages(["f"], [ILog("3", 20, 30), FLog("4", 200, 300)]),
            LogImages(["f"], [ILog("5", 2, 3)]),
            LogImages(["f"], [ILog("8", 20, 30), FLog("1000", 200, 300)]),

            LogImages(["g"], [ILog("10", 20, 30), FLog("100", 200, 300)]),
            ClearImageLog(["g"]),

            AssignString(["h"], "44"),
            LogFloats(["h"], [FLog(10, 20, 30), FLog(100, 200, 300)]),

            LogFloats(["i"], [FLog(1, 2, 3)]),
            ConfigFloatSeries(["i"], min=7, max=70, unit="%"),
            ClearFloatLog(["i"]),
            LogFloats(["i"], [FLog(10, 20, 30), FLog(100, 200, 300)]),
        ])

        # then
        self.assertEqual(processor.get_operations(), [
            LogFloats(["a"], [FLog(1, 2, 3)]),
            DeleteAttribute(["a"]),
            LogStrings(["a"], [FLog("111", 3, 4)]),

            DeleteAttribute(["b"]),
            LogStrings(["b"], [SLog("222", None, 6)]),

            LogFloats(["c"], [FLog(1, 2, 3), FLog(10, 20, 30), FLog(100, 200, 300)]),

            LogStrings(["d"], [SLog("4", 111, 222)]),

            ClearFloatLog(["e"]),

            ClearImageLog(["f"]),
            LogImages(["f"], [ILog("3", 20, 30), FLog("4", 200, 300), ILog("5", 2, 3),
                              ILog("8", 20, 30), FLog("1000", 200, 300)]),

            ClearImageLog(["g"]),

            AssignString(["h"], "44"),

            ClearFloatLog(["i"]),
            LogFloats(["i"], [FLog(10, 20, 30), FLog(100, 200, 300)]),
            ConfigFloatSeries(["i"], min=7, max=70, unit="%"),
        ])
        self.assertEqual(processor.get_errors(), [
            MetadataInconsistency("Cannot perform LogFloats operation on h: Attribute is not a Float Series")
        ])

    def test_sets(self):
        # given
        processor = OperationsPreprocessor()

        # when
        processor.process([
            AddStrings(["a"], {"xx", "y", "abc"}),
            DeleteAttribute(["a"]),
            AddStrings(["a"], {"hhh", "gij"}),

            DeleteAttribute(["b"]),
            RemoveStrings(["b"], {"abc", "defgh"}),

            AddStrings(["c"], {"hhh", "gij"}),
            RemoveStrings(["c"], {"abc", "defgh"}),
            AddStrings(["c"], {"qqq"}),

            ClearStringSet(["d"]),

            RemoveStrings(["e"], {"abc", "defgh"}),
            AddStrings(["e"], {"qqq"}),
            ClearStringSet(["e"]),

            AddStrings(["f"], {"hhh", "gij"}),
            RemoveStrings(["f"], {"abc", "defgh"}),
            AddStrings(["f"], {"qqq"}),
            ClearStringSet(["f"]),
            AddStrings(["f"], {"xx", "y", "abc"}),
            RemoveStrings(["f"], {"abc", "defgh"}),

            AssignString(["h"], "44"),
            RemoveStrings(["h"], {""}),

            AssignFloat(["i"], 5),
            AddStrings(["i"], {""}),
        ])

        # then
        self.assertEqual(processor.get_operations(), [
            AddStrings(["a"], {"xx", "y", "abc"}),
            DeleteAttribute(["a"]),
            AddStrings(["a"], {"hhh", "gij"}),

            DeleteAttribute(["b"]),
            RemoveStrings(["b"], {"abc", "defgh"}),

            AddStrings(["c"], {"hhh", "gij"}),
            RemoveStrings(["c"], {"abc", "defgh"}),
            AddStrings(["c"], {"qqq"}),

            ClearStringSet(["d"]),

            ClearStringSet(["e"]),

            ClearStringSet(["f"]),
            AddStrings(["f"], {"xx", "y", "abc"}),
            RemoveStrings(["f"], {"abc", "defgh"}),

            AssignString(["h"], "44"),
            AssignFloat(["i"], 5),
        ])
        self.assertEqual(processor.get_errors(), [
            MetadataInconsistency("Cannot perform RemoveStrings operation on h: Attribute is not a String Set"),
            MetadataInconsistency("Cannot perform AddStrings operation on i: Attribute is not a String Set")
        ])

    def test_file_set(self):
        # given
        processor = OperationsPreprocessor()

        # when
        processor.process([
            UploadFileSet(["a"], ["xx", "y", "abc"], reset=False),
            DeleteAttribute(["a"]),
            UploadFileSet(["a"], ["hhh", "gij"], reset=False),

            DeleteAttribute(["b"]),
            UploadFileSet(["b"], ["abc", "defgh"], reset=True),

            UploadFileSet(["c"], ["hhh", "gij"], reset=False),
            UploadFileSet(["c"], ["abc", "defgh"], reset=True),
            UploadFileSet(["c"], ["qqq"], reset=False),

            UploadFileSet(["d"], ["hhh", "gij"], reset=False),

            AssignFloat(["e"], 5),
            UploadFileSet(["e"], [""], reset=False),
        ])

        # then
        self.assertEqual(processor.get_operations(), [
            UploadFileSet(["a"], ["xx", "y", "abc"], reset=False),
            DeleteAttribute(["a"]),
            UploadFileSet(["a"], ["hhh", "gij"], reset=False),

            DeleteAttribute(["b"]),
            UploadFileSet(["b"], ["abc", "defgh"], reset=True),

            UploadFileSet(["c"], ["abc", "defgh"], reset=True),
            UploadFileSet(["c"], ["qqq"], reset=False),

            UploadFileSet(["d"], ["hhh", "gij"], reset=False),

            AssignFloat(["e"], 5),
        ])
        self.assertEqual(processor.get_errors(), [
            MetadataInconsistency("Cannot perform UploadFileSet operation on e: Attribute is not a File Set")
        ])
