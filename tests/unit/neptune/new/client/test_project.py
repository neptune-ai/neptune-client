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
from datetime import datetime

from mock import patch

from neptune import (
    ANONYMOUS_API_TOKEN,
    init_project,
)
from neptune.common.exceptions import NeptuneException
from neptune.common.warnings import (
    NeptuneWarning,
    warned_once,
)
from neptune.envs import (
    API_TOKEN_ENV_NAME,
    PROJECT_ENV_NAME,
)
from neptune.exceptions import NeptuneMissingProjectNameException
from neptune.internal.backends.api_model import (
    Attribute,
    AttributeType,
    AttributeWithProperties,
    IntAttribute,
    LeaderboardEntry,
)
from neptune.internal.backends.neptune_backend_mock import NeptuneBackendMock
from neptune.metadata_containers.utils import (
    DATE_FORMAT,
    parse_dates,
    prepare_nql_query,
)
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
    @patch("neptune.internal.operation_processors.read_only_operation_processor.warn_once")
    def test_read_only_mode(self, warn_once):
        warned_once.clear()
        with init_project(project=self.PROJECT_NAME, mode="read-only") as project:
            project["some/variable"] = 13
            project["some/other_variable"] = 11

            warn_once.assert_called_with(
                "Client in read-only mode, nothing will be saved to server.", exception=NeptuneWarning
            )

            self.assertEqual(42, project["some/variable"].fetch())
            self.assertNotIn(str(project._id), os.listdir(".neptune"))


def test_prepare_nql_query():
    query = prepare_nql_query(
        ["id1", "id2"],
        ["active"],
        ["owner1", "owner2"],
        ["tag1", "tag2"],
        trashed=True,
    )
    assert len(query.items) == 5

    query = prepare_nql_query(
        ["id1", "id2"],
        ["active"],
        ["owner1", "owner2"],
        ["tag1", "tag2"],
        trashed=None,
    )
    assert len(query.items) == 4

    query = prepare_nql_query(
        None,
        None,
        None,
        None,
        trashed=None,
    )
    assert len(query.items) == 0


def test_parse_dates():
    def entries_generator():
        yield LeaderboardEntry(
            id="test",
            attributes=[
                AttributeWithProperties(
                    "attr1",
                    AttributeType.DATETIME,
                    {"value": datetime(2024, 2, 5, 20, 37, 40, 915000).strftime(DATE_FORMAT)},
                ),
                AttributeWithProperties(
                    "attr2",
                    AttributeType.DATETIME,
                    {"value": datetime(2024, 2, 5, 20, 37, 40, 915000).strftime(DATE_FORMAT)},
                ),
            ],
        )

    parsed = list(parse_dates(entries_generator()))
    assert parsed[0].attributes[0].properties["value"] == datetime(2024, 2, 5, 20, 37, 40, 915000)
    assert parsed[0].attributes[1].properties["value"] == datetime(2024, 2, 5, 20, 37, 40, 915000)


@patch("neptune.metadata_containers.utils.warn_once")
def test_parse_dates_wrong_format(mock_warn_once):
    entries = [
        LeaderboardEntry(
            id="test",
            attributes=[
                AttributeWithProperties(
                    "attr1",
                    AttributeType.DATETIME,
                    {"value": "07-02-2024"},  # different format than expected
                )
            ],
        )
    ]

    parsed = list(parse_dates(entries))
    assert parsed[0].attributes[0].properties["value"] == "07-02-2024"  # should be left unchanged due to ValueError
    mock_warn_once.assert_called_once_with(
        "Date parsing failed. The date format is incorrect. Returning as string instead of datetime.",
        exception=NeptuneWarning,
    )
