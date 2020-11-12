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

import os
import unittest
import uuid

from mock import patch

from neptune.alpha import init, ANONYMOUS
from neptune.alpha.attributes.atoms import String
from neptune.alpha.envs import PROJECT_ENV_NAME, API_TOKEN_ENV_NAME
from neptune.alpha.exceptions import MetadataInconsistency
from neptune.alpha.internal.backends.api_model import Experiment, Attribute, AttributeType
from neptune.alpha.internal.backends.neptune_backend_mock import NeptuneBackendMock


@patch('neptune.alpha.internal.init_impl.HostedNeptuneBackend', NeptuneBackendMock)
class TestClient(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        os.environ[PROJECT_ENV_NAME] = "organization/project"
        os.environ[API_TOKEN_ENV_NAME] = ANONYMOUS

    def test_incorrect_mode(self):
        with self.assertRaises(ValueError):
            init(connection_mode='srtgj')

    def test_debug_mode(self):
        exp = init(connection_mode='debug')
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

    @patch("neptune.alpha.internal.backends.neptune_backend_mock.NeptuneBackendMock.get_experiment",
           new=lambda _, _id:
           Experiment(uuid.UUID('12345678-1234-5678-1234-567812345678'), "SAN-94", "workspace", "sandbox"))
    @patch("neptune.alpha.internal.backends.neptune_backend_mock.NeptuneBackendMock.get_attributes",
           new=lambda _, _uuid: [Attribute("test", AttributeType.STRING)])
    def test_resume(self):
        exp = init(flush_period=0.5, experiment="SAN-94")
        self.assertEqual(exp._uuid, uuid.UUID('12345678-1234-5678-1234-567812345678'))
        self.assertIsInstance(exp.get_structure()["test"], String)
