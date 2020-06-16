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

from neptune.internal.backends.offline_backend import NoopObject

class TestHostedNeptuneObject(unittest.TestCase):

    # given
    objectUnderTest = NoopObject()
    some_value = 42

    def test_builtin_fields_not_overriden(self):
        #when
        result = self.objectUnderTest.__class__

        # then
        self.assertIsInstance(result, type)

    def test_attributes_fall_back_on_getattr(self):
        #when
        attribute = self.objectUnderTest.foo

        # and
        method_call = self.objectUnderTest.bar(self.some_value)

        # then
        self.assertEqual(attribute, self.objectUnderTest)

        # and
        self.assertEqual(method_call, self.objectUnderTest)

    def test_noop_object_callable(self):
        # when
        result = self.objectUnderTest(self.some_value)

        # then
        self.assertEqual(result, self.objectUnderTest)

    def test_noop_object_context_manager(self):
        # when
        with self.objectUnderTest as e:

            # then
            e(42)
