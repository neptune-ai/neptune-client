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
import os
import unittest

from neptune import init

# pylint: disable=protected-access
from neptune.exceptions import MetadataInconsistency


class TestClient(unittest.TestCase):

    def test_incorrect_mode(self):
        with self.assertRaises(ValueError):
            init(connection_mode='srtgj')

    def test_offline_mode(self):
        exp = init(connection_mode='offline')
        exp["some/variable"] = 13
        self.assertEqual(13, exp["some/variable"].get())
        self.assertNotIn(str(exp._uuid), os.listdir(".neptune"))

    def test_sync_mode(self):
        exp = init(connection_mode='sync')
        exp["some/variable"] = 13
        self.assertEqual(13, exp["some/variable"].get())
        self.assertNotIn(str(exp._uuid), os.listdir(".neptune"))

    def test_async_mode(self):
        exp = init(connection_mode='async', flush_period=0.5)
        exp["some/variable"] = 13
        # TODO: Should be None or exception?
        # self.assertEqual(None, exp["some/variable"].get())
        with self.assertRaises(MetadataInconsistency):
            exp["some/variable"].get()
        exp.wait()
        self.assertEqual(13, exp["some/variable"].get())
        self.assertIn(str(exp._uuid), os.listdir(".neptune"))
        self.assertIn("operations-0.log", os.listdir(".neptune/{}".format(exp._uuid)))
