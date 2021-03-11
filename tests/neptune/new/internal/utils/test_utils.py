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

import unittest

from neptune.new.internal.utils import verify_type, verify_collection_type


class TestUtils(unittest.TestCase):

    def test_verify_type(self):
        verify_type("arg", "string", str)

    def test_verify_type_failed(self):
        with self.assertRaises(TypeError):
            verify_type("arg", 5, str)

    def test_verify_type_tuple(self):
        verify_type("arg", "string", (int, float, str))

    def test_verify_type_tuple_failed(self):
        with self.assertRaises(TypeError):
            verify_type("arg", 5, (str, type(None), float))

    def test_verify_collection_type(self):
        verify_collection_type("arg", ["string", "aaa", 5, 1, "q"], (int, str))

    def test_verify_collection_type_failed(self):
        with self.assertRaises(TypeError):
            verify_collection_type("arg", "string", (int, str))

    def test_verify_collection_type_failed_element(self):
        with self.assertRaises(TypeError):
            verify_collection_type("arg", ["string", 3, "a", 4., 1], (int, str))
