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
from datetime import datetime

from mock import Mock, patch

from neptune.new import ANONYMOUS, get_project, init_project
from neptune.new.envs import API_TOKEN_ENV_NAME, PROJECT_ENV_NAME
from neptune.new.exceptions import (
    MetadataInconsistency,
    NeptuneException,
    NeptuneMissingProjectNameException,
)
from neptune.new.internal.backends.api_model import (
    Attribute,
    AttributeType,
    AttributeWithProperties,
    IntAttribute,
    LeaderboardEntry,
)
from neptune.new.internal.backends.neptune_backend_mock import NeptuneBackendMock
from neptune.new.internal.container_type import ContainerType
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

    @staticmethod
    def build_attributes_leaderboard(now: datetime):
        attributes = []
        attributes.append(
            AttributeWithProperties(
                "run/state", AttributeType.RUN_STATE, Mock(value="idle")
            )
        )
        attributes.append(
            AttributeWithProperties("float", AttributeType.FLOAT, Mock(value=12.5))
        )
        attributes.append(
            AttributeWithProperties(
                "string", AttributeType.STRING, Mock(value="some text")
            )
        )
        attributes.append(
            AttributeWithProperties("datetime", AttributeType.DATETIME, Mock(value=now))
        )
        attributes.append(
            AttributeWithProperties(
                "float/series", AttributeType.FLOAT_SERIES, Mock(last=8.7)
            )
        )
        attributes.append(
            AttributeWithProperties(
                "string/series", AttributeType.STRING_SERIES, Mock(last="last text")
            )
        )
        attributes.append(
            AttributeWithProperties(
                "string/set", AttributeType.STRING_SET, Mock(values=["a", "b"])
            )
        )
        attributes.append(
            AttributeWithProperties(
                "git/ref",
                AttributeType.GIT_REF,
                Mock(commit=Mock(commitId="abcdef0123456789")),
            )
        )
        attributes.append(AttributeWithProperties("file", AttributeType.FILE, None))
        attributes.append(
            AttributeWithProperties("file/set", AttributeType.FILE_SET, None)
        )
        attributes.append(
            AttributeWithProperties("image/series", AttributeType.IMAGE_SERIES, None)
        )
        return attributes

    @patch.object(NeptuneBackendMock, "get_leaderboard")
    def test_get_table_as_pandas(self, get_leaderboard):
        # given
        now = datetime.now()
        attributes = self.build_attributes_leaderboard(now)

        # and
        empty_entry = LeaderboardEntry(str(uuid.uuid4()), [])
        filled_entry = LeaderboardEntry(str(uuid.uuid4()), attributes)
        get_leaderboard.return_value = [empty_entry, filled_entry]

        # when
        df = get_project(self.PROJECT_NAME).fetch_runs_table().to_pandas()

        # then
        self.assertEqual("idle", df["run/state"][1])
        self.assertEqual(12.5, df["float"][1])
        self.assertEqual("some text", df["string"][1])
        self.assertEqual(now, df["datetime"][1])
        self.assertEqual(8.7, df["float/series"][1])
        self.assertEqual("last text", df["string/series"][1])
        self.assertEqual("a,b", df["string/set"][1])
        self.assertEqual("abcdef0123456789", df["git/ref"][1])

        with self.assertRaises(KeyError):
            self.assertTrue(df["file"])
        with self.assertRaises(KeyError):
            self.assertTrue(df["file/set"])
        with self.assertRaises(KeyError):
            self.assertTrue(df["image/series"])

    @patch.object(NeptuneBackendMock, "get_leaderboard")
    @patch.object(NeptuneBackendMock, "download_file")
    @patch.object(NeptuneBackendMock, "download_file_set")
    def test_get_table_as_runs(self, download_file_set, download_file, get_leaderboard):
        # given
        exp_id = str(uuid.uuid4())
        now = datetime.now()
        attributes = self.build_attributes_leaderboard(now)

        # and
        get_leaderboard.return_value = [LeaderboardEntry(exp_id, attributes)]

        # when
        exp = get_project(self.PROJECT_NAME).fetch_runs_table().to_runs()[0]

        # then
        self.assertEqual("idle", exp["run/state"].get())
        self.assertEqual("idle", exp["run"]["state"].get())
        self.assertEqual(12.5, exp["float"].get())
        self.assertEqual("some text", exp["string"].get())
        self.assertEqual(now, exp["datetime"].get())
        self.assertEqual(8.7, exp["float/series"].get())
        self.assertEqual("last text", exp["string/series"].get())
        self.assertEqual({"a", "b"}, exp["string/set"].get())
        self.assertEqual("abcdef0123456789", exp["git/ref"].get())

        with self.assertRaises(MetadataInconsistency):
            exp["file"].get()
        with self.assertRaises(MetadataInconsistency):
            exp["file/set"].get()
        with self.assertRaises(MetadataInconsistency):
            exp["image/series"].get()

        exp["file"].download("some_directory")
        download_file.assert_called_with(
            exp_id, ContainerType.RUN, ["file"], "some_directory"
        )

        exp["file/set"].download("some_directory")
        download_file_set.assert_called_with(
            exp_id, ContainerType.RUN, ["file", "set"], "some_directory"
        )
