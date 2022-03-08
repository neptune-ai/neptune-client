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

from neptune.new import ANONYMOUS, init_model
from neptune.new.attributes import String
from neptune.new.envs import API_TOKEN_ENV_NAME, PROJECT_ENV_NAME
from neptune.new.exceptions import NeptuneWrongInitParametersException, NeptuneException
from neptune.new.internal.backends.api_model import (
    Attribute,
    AttributeType,
    IntAttribute,
)
from neptune.new.internal.backends.neptune_backend_mock import NeptuneBackendMock
from tests.neptune.new.client.abstract_experiment_test_mixin import (
    AbstractExperimentTestMixin,
)
from tests.neptune.new.utils.api_experiments_factory import api_model

AN_API_MODEL = api_model()


@patch("neptune.new.internal.backends.factory.HostedNeptuneBackend", NeptuneBackendMock)
class TestClientModel(AbstractExperimentTestMixin, unittest.TestCase):
    @staticmethod
    def call_init(**kwargs):
        return init_model(key="MOD", **kwargs)

    @classmethod
    def setUpClass(cls) -> None:
        os.environ[PROJECT_ENV_NAME] = "organization/project"
        os.environ[API_TOKEN_ENV_NAME] = ANONYMOUS

    def test_offline_mode(self):
        with self.assertRaises(NeptuneException):
            init_model(key="MOD", mode="offline")

    @patch(
        "neptune.new.internal.backends.neptune_backend_mock.NeptuneBackendMock.get_metadata_container",
        new=lambda _, container_id, expected_container_type: AN_API_MODEL,
    )
    @patch(
        "neptune.new.internal.backends.neptune_backend_mock.NeptuneBackendMock.get_attributes",
        new=lambda _, _uuid, _type: [Attribute("some/variable", AttributeType.INT)],
    )
    @patch(
        "neptune.new.internal.backends.neptune_backend_mock.NeptuneBackendMock.get_int_attribute",
        new=lambda _, _uuid, _type, _path: IntAttribute(42),
    )
    def test_read_only_mode(self):
        exp = init_model(mode="read-only", model="whatever")

        with self.assertLogs() as caplog:
            exp["some/variable"] = 13
            exp["some/other_variable"] = 11
            self.assertEqual(
                caplog.output,
                [
                    "WARNING:neptune.new.internal.operation_processors.read_only_operation_processor:"
                    "Client in read-only mode, nothing will be saved to server."
                ],
            )

        self.assertEqual(42, exp["some/variable"].fetch())
        self.assertNotIn(str(exp._id), os.listdir(".neptune"))

    @patch(
        "neptune.new.internal.backends.neptune_backend_mock.NeptuneBackendMock.get_metadata_container",
        new=lambda _, container_id, expected_container_type: AN_API_MODEL,
    )
    @patch(
        "neptune.new.internal.backends.neptune_backend_mock.NeptuneBackendMock.get_attributes",
        new=lambda _, _uuid, _type: [Attribute("test", AttributeType.STRING)],
    )
    def test_resume(self):
        with init_model(flush_period=0.5, model="whatever") as exp:
            self.assertEqual(exp._id, AN_API_MODEL.id)
            self.assertIsInstance(exp.get_structure()["test"], String)

    def test_wrong_parameters(self):
        with self.assertRaises(NeptuneWrongInitParametersException):
            init_model(model=None, key=None)

    def test_name_parameter(self):
        with init_model(key="TRY", name="some_name") as exp:
            exp.wait()
            self.assertEqual(exp["sys/name"].fetch(), "some_name")
