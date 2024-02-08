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
from unittest.mock import (
    MagicMock,
    call,
    patch,
)

import pytest
from bravado.exception import HTTPNotFound
from bravado.response import BravadoResponse

from neptune import ANONYMOUS_API_TOKEN
from neptune.common.envs import API_TOKEN_ENV_NAME
from neptune.envs import PROJECT_ENV_NAME
from neptune.internal.backends.hosted_client import DEFAULT_REQUEST_KWARGS
from neptune.internal.backends.neptune_backend_mock import NeptuneBackendMock
from neptune.management import (
    clear_trash,
    delete_objects_from_trash,
    trash_objects,
)
from neptune.management.exceptions import ProjectNotFound


@patch("neptune.internal.backends.factory.HostedNeptuneBackend", NeptuneBackendMock)
class TestTrashObjects(unittest.TestCase):
    PROJECT_NAME = "organization/project"

    @classmethod
    def setUpClass(cls) -> None:
        os.environ[API_TOKEN_ENV_NAME] = ANONYMOUS_API_TOKEN

    @classmethod
    def setUp(cls) -> None:
        if PROJECT_ENV_NAME in os.environ:
            del os.environ[PROJECT_ENV_NAME]

    @patch("neptune.management.internal.api.logger")
    @patch("neptune.management.internal.api._get_leaderboard_client")
    def test_project_trash_objects(self, _get_leaderboard_client_mock, _mock_logger):
        # given
        trash_experiments_mock = _get_leaderboard_client_mock().api.trashExperiments

        mock_response = MagicMock(spec=BravadoResponse(MagicMock(), MagicMock()))
        trash_experiments_mock.return_value.response.return_value = mock_response

        mock_response.result.errors = ["some_test_error1", "some_test_error2"]
        mock_response.result.updatedExperimentIdentifiers = ["RUN-1"]

        # when
        trash_objects(self.PROJECT_NAME, ["RUN-1", "MOD", "MOD-1"])

        # then
        self.assertEqual(1, trash_experiments_mock.call_count)
        self.assertEqual(
            call(
                projectIdentifier="organization/project",
                experimentIdentifiers=[
                    "organization/project/RUN-1",
                    "organization/project/MOD",
                    "organization/project/MOD-1",
                ],
                **DEFAULT_REQUEST_KWARGS,
            ),
            trash_experiments_mock.call_args,
        )
        _mock_logger.info.assert_called_once_with("Successfully trashed objects: %d. Number of failures: %d.", 1, 2)
        self.assertEqual(_mock_logger.warning.mock_calls, [call("some_test_error1"), call("some_test_error2")])

    @patch("neptune.management.internal.api._get_leaderboard_client")
    def test_trash_objects_invalid_project_name(self, _get_leaderboard_client_mock):
        _get_leaderboard_client_mock().api.trashExperiments.side_effect = HTTPNotFound(MagicMock())
        with pytest.raises(ProjectNotFound):
            trash_objects(self.PROJECT_NAME, ["RUN-1", "MOD", "MOD-1"])

    @patch("neptune.management.internal.api._get_leaderboard_client")
    def test_project_delete_objects_from_trash(self, _get_leaderboard_client_mock):
        # given
        delete_experiments_from_trash_mock = _get_leaderboard_client_mock().api.deleteExperiments

        # when
        delete_objects_from_trash(self.PROJECT_NAME, ["RUN-1", "MOD", "MOD-1"])

        # then
        assert delete_experiments_from_trash_mock.call_count == 1
        self.assertEqual(
            call(
                projectIdentifier="organization/project",
                experimentIdentifiers=[
                    "organization/project/RUN-1",
                    "organization/project/MOD",
                    "organization/project/MOD-1",
                ],
                **DEFAULT_REQUEST_KWARGS,
            ),
            delete_experiments_from_trash_mock.call_args,
        )

    @patch("neptune.management.internal.api._get_leaderboard_client")
    def test_project_clear_trash(self, _get_leaderboard_client_mock):
        # given
        clear_trash_mock = _get_leaderboard_client_mock().api.deleteAllExperiments

        # when
        clear_trash(self.PROJECT_NAME)

        # then
        assert clear_trash_mock.call_count == 1
        self.assertEqual(
            call(
                projectIdentifier="organization/project",
                **DEFAULT_REQUEST_KWARGS,
            ),
            clear_trash_mock.call_args,
        )
