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

from mock import patch

from neptune.new import ANONYMOUS, init_model_version
from neptune.new.attributes import String
from neptune.new.envs import API_TOKEN_ENV_NAME, PROJECT_ENV_NAME
from neptune.new.exceptions import (
    NeptuneOfflineModeChangeStageException,
    NeptuneWongInitParametersException,
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
from tests.neptune.new.utils.api_experiments_factory import api_model, api_model_version

AN_API_MODEL = api_model()
AN_API_MODEL_VERSION = api_model_version()


@patch(
    "neptune.new.internal.backends.neptune_backend_mock.NeptuneBackendMock.get_model",
    new=lambda _, model_id: AN_API_MODEL,
)
@patch("neptune.new.internal.backends.factory.HostedNeptuneBackend", NeptuneBackendMock)
class TestClientModelVersion(AbstractExperimentTestMixin, unittest.TestCase):
    @staticmethod
    def call_init(**kwargs):
        return init_model_version(model="PRO-MOD", **kwargs)

    @classmethod
    def setUpClass(cls) -> None:
        os.environ[PROJECT_ENV_NAME] = "organization/project"
        os.environ[API_TOKEN_ENV_NAME] = ANONYMOUS

    @patch(
        "neptune.new.internal.backends.neptune_backend_mock.NeptuneBackendMock.get_model_version",
        new=lambda _, model_version_id: AN_API_MODEL_VERSION,
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
        exp = init_model_version(mode="read-only", version="whatever")

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
        "neptune.new.internal.backends.neptune_backend_mock.NeptuneBackendMock.get_model_version",
        new=lambda _, model_version_id: AN_API_MODEL_VERSION,
    )
    @patch(
        "neptune.new.internal.backends.neptune_backend_mock.NeptuneBackendMock.get_attributes",
        new=lambda _, _uuid, _type: [Attribute("test", AttributeType.STRING)],
    )
    def test_resume(self):
        with init_model_version(flush_period=0.5, version="whatever") as exp:
            self.assertEqual(exp._id, AN_API_MODEL_VERSION.id)
            self.assertIsInstance(exp.get_structure()["test"], String)

    def test_wrong_parameters(self):
        with self.assertRaises(NeptuneWongInitParametersException):
            init_model_version(version=None, model=None)
        with self.assertRaises(NeptuneWongInitParametersException):
            init_model_version(version="whatever", model="whatever")

    def test_change_stage(self):
        exp = self.call_init()
        exp.change_stage(stage="production")

        self.assertEqual("production", exp["sys/stage"].fetch())

        with self.assertRaises(NeptuneOfflineModeChangeStageException):
            exp.change_stage(stage="wrong_stage")

    def test_change_stage_of_offline_model_version(self):
        exp = self.call_init(mode="offline")
        with self.assertRaises(NeptuneOfflineModeChangeStageException):
            exp.change_stage(stage="production")
