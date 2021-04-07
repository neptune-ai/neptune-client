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

import unittest

from neptune.utils import NoopObject


class TestHostedNeptuneObject(unittest.TestCase):

    def test_builtin_fields_not_overriden(self):
        # given
        objectUnderTest = NoopObject()

        # then
        self.assertIsInstance(objectUnderTest.__class__, type)

    def test_attributes_fall_back_on_getattr(self):
        # given
        objectUnderTest = NoopObject()
        some_value = 42

        # then
        self.assertEqual(objectUnderTest.foo, objectUnderTest)
        self.assertEqual(objectUnderTest.bar(some_value), objectUnderTest)

    def test_attributes_fall_back_on_getitem(self):
        # given
        objectUnderTest = NoopObject()
        some_value = 42

        # then
        self.assertEqual(objectUnderTest['foo'], objectUnderTest)
        self.assertEqual(objectUnderTest['bar'](some_value), objectUnderTest)

    def test_noop_object_callable(self):
        # given
        objectUnderTest = NoopObject()
        some_value = 42

        # then
        self.assertEqual(objectUnderTest(some_value), objectUnderTest)

    def test_noop_object_context_manager(self):
        # given
        objectUnderTest = NoopObject()

        # when
        with objectUnderTest as e:

            # then
            e(42)
