#
# Copyright (c) 2022, Neptune Labs Sp. z o.o.
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

from mock import patch

from neptune.new import ANONYMOUS, init_project
from neptune.new.envs import API_TOKEN_ENV_NAME, PROJECT_ENV_NAME
from neptune.new.exceptions import (
    NeptuneException,
    NeptuneMissingProjectNameException,
)
from neptune.new.internal.backends.api_model import (
    Attribute,
    AttributeType,
    IntAttribute,
)
from neptune.new.internal.backends.neptune_backend_mock import NeptuneBackendMock
from tests.neptune.new.client.abstract_experiment_test_mixin import (
    AbstractExperimentTestMixin,
)


@patch(
    "neptune.new.internal.backends.neptune_backend_mock.NeptuneBackendMock.get_attributes",
    new=lambda _, _uuid, _type: [Attribute("test", AttributeType.STRING)],
)
@patch("neptune.new.internal.backends.factory.HostedNeptuneBackend", NeptuneBackendMock)
class TestClientProject(AbstractExperimentTestMixin, unittest.TestCase):
    PROJECT_NAME = "organization/project"

    @staticmethod
    def call_init(**kwargs):
        return init_project(name=TestClientProject.PROJECT_NAME, **kwargs)

    @classmethod
    def setUpClass(cls) -> None:
        os.environ[API_TOKEN_ENV_NAME] = ANONYMOUS

    @classmethod
    def setUp(cls) -> None:
        if PROJECT_ENV_NAME in os.environ:
            del os.environ[PROJECT_ENV_NAME]

    def test_offline_mode(self):
        with self.assertRaises(NeptuneException):
            init_project(name=self.PROJECT_NAME, mode="offline")

    def test_no_project_name(self):
        with self.assertRaises(NeptuneMissingProjectNameException):
            init_project(mode="async")

    def test_inexistent_project(self):
        with self.assertRaises(NeptuneMissingProjectNameException):
            init_project(mode="async")

    def test_project_name_env_var(self):
        os.environ[PROJECT_ENV_NAME] = self.PROJECT_NAME

        project = init_project(mode="sync")
        project["some/variable"] = 13
        self.assertEqual(13, project["some/variable"].fetch())

    @patch(
        "neptune.new.internal.backends.neptune_backend_mock.NeptuneBackendMock.get_int_attribute",
        new=lambda _, _uuid, _type, _path: IntAttribute(42),
    )
    def test_read_only_mode(self):
        project = init_project(name=self.PROJECT_NAME, mode="read-only")

        with self.assertLogs() as caplog:
            project["some/variable"] = 13
            project["some/other_variable"] = 11
            self.assertEqual(
                caplog.output,
                [
                    "WARNING:neptune.new.internal.operation_processors.read_only_operation_processor:"
                    "Client in read-only mode, nothing will be saved to server."
                ],
            )

        self.assertEqual(42, project["some/variable"].fetch())
        self.assertNotIn(str(project._id), os.listdir(".neptune"))
