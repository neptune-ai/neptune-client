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
import os
import unittest

from mock import patch

from neptune import (
    ANONYMOUS_API_TOKEN,
    init_project,
)
from neptune.common.exceptions import NeptuneException
from neptune.envs import (
    API_TOKEN_ENV_NAME,
    PROJECT_ENV_NAME,
)
from neptune.exceptions import NeptuneMissingProjectNameException
from neptune.internal.backends.api_model import (
    Attribute,
    AttributeType,
    IntAttribute,
)
from neptune.internal.backends.neptune_backend_mock import NeptuneBackendMock
from tests.unit.neptune.new.client.abstract_experiment_test_mixin import AbstractExperimentTestMixin


@patch(
    "neptune.internal.backends.neptune_backend_mock.NeptuneBackendMock.get_attributes",
    new=lambda _, _uuid, _type: [Attribute("test", AttributeType.STRING)],
)
@patch("neptune.internal.backends.factory.HostedNeptuneBackend", NeptuneBackendMock)
class TestClientProject(AbstractExperimentTestMixin, unittest.TestCase):
    PROJECT_NAME = "organization/project"

    @staticmethod
    def call_init(**kwargs):
        return init_project(project=TestClientProject.PROJECT_NAME, **kwargs)

    @classmethod
    def setUpClass(cls) -> None:
        os.environ[API_TOKEN_ENV_NAME] = ANONYMOUS_API_TOKEN

    @classmethod
    def setUp(cls) -> None:
        if PROJECT_ENV_NAME in os.environ:
            del os.environ[PROJECT_ENV_NAME]

    def test_offline_mode(self):
        with self.assertRaises(NeptuneException):
            with init_project(project=self.PROJECT_NAME, mode="offline"):
                pass

    def test_no_project_name(self):
        with self.assertRaises(NeptuneMissingProjectNameException):
            with init_project(mode="async"):
                pass

    def test_inexistent_project(self):
        with self.assertRaises(NeptuneMissingProjectNameException):
            with init_project(mode="async"):
                pass

    def test_project_name_env_var(self):
        os.environ[PROJECT_ENV_NAME] = self.PROJECT_NAME

        with init_project(mode="sync") as project:
            project["some/variable"] = 13
            self.assertEqual(13, project["some/variable"].fetch())

    @patch(
        "neptune.internal.backends.neptune_backend_mock.NeptuneBackendMock.get_int_attribute",
        new=lambda _, _uuid, _type, _path: IntAttribute(42),
    )
    def test_read_only_mode(self):
        with init_project(project=self.PROJECT_NAME, mode="read-only") as project:
            with self.assertLogs() as caplog:
                project["some/variable"] = 13
                project["some/other_variable"] = 11
                self.assertEqual(
                    caplog.output,
                    [
                        "WARNING:neptune.internal.operation_processors.read_only_operation_processor:"
                        "Client in read-only mode, nothing will be saved to server."
                    ],
                )

            self.assertEqual(42, project["some/variable"].fetch())
            self.assertNotIn(str(project._id), os.listdir(".neptune"))
