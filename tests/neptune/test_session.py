#
# Copyright (c) 2019, Neptune Labs Sp. z o.o.
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

import unittest
from collections import OrderedDict

from mock import MagicMock, patch

from neptune.projects import Project
from neptune.sessions import Session
from tests.neptune.api_objects_factory import a_project


@patch('neptune.internal.api_clients.hosted_api_clients.mixins.SwaggerClient.from_url',
       MagicMock())
@patch('neptune.internal.api_clients.hosted_api_clients.hosted_backend_api_client.NeptuneAuthenticator',
       MagicMock())
class TestSession(unittest.TestCase):

    # threading.RLock needs to be mocked, because it breaks the equality of Projects
    @patch('threading.RLock')
    def test_get_projects_with_given_namespace(self, _):
        # given
        api_projects = [a_project(), a_project()]

        # and
        backend = MagicMock()
        leaderboard = MagicMock()
        backend.get_projects.return_value = api_projects
        backend.create_leaderboard_backend.return_value = leaderboard

        # and
        session = Session(backend=backend)

        # and
        custom_namespace = 'custom_namespace'

        # when
        projects = session.get_projects(custom_namespace)

        # then
        expected_projects = OrderedDict(
            (custom_namespace + '/' + p.name, Project(leaderboard, p.id, custom_namespace, p.name))
            for p in api_projects
        )
        self.assertEqual(expected_projects, projects)

        # and
        backend.get_projects.assert_called_with(custom_namespace)


if __name__ == '__main__':
    unittest.main()
