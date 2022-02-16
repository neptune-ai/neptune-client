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

from neptune.new import ANONYMOUS, Run, get_last_run, init_run
from neptune.new.attributes.atoms import String
from neptune.new.envs import API_TOKEN_ENV_NAME, PROJECT_ENV_NAME
from neptune.new.exceptions import MissingFieldException, NeptuneUninitializedException
from neptune.new.internal.backends.api_model import (
    Attribute,
    AttributeType,
    IntAttribute,
)
from neptune.new.internal.backends.neptune_backend_mock import NeptuneBackendMock
from neptune.utils import IS_WINDOWS
from tests.neptune.new.client.abstract_experiment_test_mixin import (
    AbstractExperimentTestMixin,
)
from tests.neptune.new.utils.api_experiments_factory import api_run

AN_API_RUN = api_run()


@patch("neptune.new.internal.backends.factory.HostedNeptuneBackend", NeptuneBackendMock)
class TestClientRun(AbstractExperimentTestMixin, unittest.TestCase):
    @staticmethod
    def call_init(**kwargs):
        return init_run(**kwargs)

    @classmethod
    def setUpClass(cls) -> None:
        os.environ[PROJECT_ENV_NAME] = "organization/project"
        os.environ[API_TOKEN_ENV_NAME] = ANONYMOUS

    @patch(
        "neptune.new.internal.backends.neptune_backend_mock.NeptuneBackendMock.get_metadata_container",
        new=lambda _, container_id, expected_container_type: AN_API_RUN,
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
        exp = init_run(mode="read-only", run="whatever")

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
        new=lambda _, container_id, expected_container_type: AN_API_RUN,
    )
    @patch(
        "neptune.new.internal.backends.neptune_backend_mock.NeptuneBackendMock.get_attributes",
        new=lambda _, _uuid, _type: [Attribute("test", AttributeType.STRING)],
    )
    def test_resume(self):
        with init_run(flush_period=0.5, run="whatever") as exp:
            self.assertEqual(exp._id, AN_API_RUN.id)
            self.assertIsInstance(exp.get_structure()["test"], String)

    @patch("neptune.new.internal.utils.source_code.sys.argv", ["main.py"])
    @patch("neptune.new.internal.init.run.os.path.isfile", new=lambda file: "." in file)
    @patch(
        "neptune.new.internal.utils.glob",
        new=lambda path, recursive=False: [path.replace("*", "file.txt")],
    )
    @patch(
        "neptune.new.internal.utils.os.path.abspath",
        new=lambda path: os.path.normpath("/home/user/main_dir/" + path),
    )
    @patch("neptune.new.internal.utils.os.getcwd", new=lambda: "/home/user/main_dir")
    @unittest.skipIf(IS_WINDOWS, "Linux/Mac test")
    def test_entrypoint(self):
        exp = init_run(mode="debug")
        self.assertEqual(exp["source_code/entrypoint"].fetch(), "main.py")

        exp = init_run(mode="debug", source_files=[])
        self.assertEqual(exp["source_code/entrypoint"].fetch(), "main.py")

        exp = init_run(mode="debug", source_files=["../*"])
        self.assertEqual(exp["source_code/entrypoint"].fetch(), "main_dir/main.py")

        exp = init_run(mode="debug", source_files=["internal/*"])
        self.assertEqual(exp["source_code/entrypoint"].fetch(), "main.py")

        exp = init_run(mode="debug", source_files=["../other_dir/*"])
        self.assertEqual(exp["source_code/entrypoint"].fetch(), "../main_dir/main.py")

    @patch("neptune.new.internal.utils.source_code.sys.argv", ["main.py"])
    @patch("neptune.new.internal.utils.source_code.is_ipython", new=lambda: True)
    def test_entrypoint_in_interactive_python(self):
        exp = init_run(mode="debug")
        with self.assertRaises(MissingFieldException):
            exp["source_code/entrypoint"].fetch()

        exp = init_run(mode="debug", source_files=[])
        with self.assertRaises(MissingFieldException):
            exp["source_code/entrypoint"].fetch()

        exp = init_run(mode="debug", source_files=["../*"])
        with self.assertRaises(MissingFieldException):
            exp["source_code/entrypoint"].fetch()

        exp = init_run(mode="debug", source_files=["internal/*"])
        with self.assertRaises(MissingFieldException):
            exp["source_code/entrypoint"].fetch()

    @patch("neptune.new.internal.utils.source_code.sys.argv", ["main.py"])
    @patch("neptune.new.internal.utils.source_code.get_common_root", new=lambda _: None)
    @patch("neptune.new.internal.init.run.os.path.isfile", new=lambda file: "." in file)
    @patch(
        "neptune.new.internal.utils.glob",
        new=lambda path, recursive=False: [path.replace("*", "file.txt")],
    )
    @patch(
        "neptune.new.internal.utils.os.path.abspath",
        new=lambda path: os.path.normpath("/home/user/main_dir/" + path),
    )
    def test_entrypoint_without_common_root(self):
        exp = init_run(mode="debug", source_files=["../*"])
        self.assertEqual(
            exp["source_code/entrypoint"].fetch(), "/home/user/main_dir/main.py"
        )

        exp = init_run(mode="debug", source_files=["internal/*"])
        self.assertEqual(
            exp["source_code/entrypoint"].fetch(), "/home/user/main_dir/main.py"
        )

    def test_last_exp_is_raising_exception_when_non_initialized(self):
        # given uninitialized run
        Run.last_run = None

        # expect: raises NeptuneUninitializedException
        with self.assertRaises(NeptuneUninitializedException):
            get_last_run()

    def test_last_exp_is_the_latest_initialized(self):
        # given two initialized runs
        with init_run() as exp1, init_run() as exp2:
            # expect: `neptune.latest_run` to be the latest initialized one
            self.assertIsNot(exp1, get_last_run())
            self.assertIs(exp2, get_last_run())
