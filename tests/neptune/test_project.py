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
from random import randint

import pandas as pd
from mock import MagicMock
from munch import Munch

from neptune.exceptions import NoExperimentContext
from neptune.experiments import Experiment
from neptune.model import LeaderboardEntry
from neptune.projects import Project
from tests.neptune.api_objects_factory import a_registered_project_member, \
    an_invited_project_member
from tests.neptune.project_test_fixture import some_exp_entry_dto, some_exp_entry_row
from tests.neptune.random_utils import a_string, a_string_list, a_uuid_string


class TestProject(unittest.TestCase):
    def setUp(self):
        super(TestProject, self).setUp()
        self.backend = MagicMock()
        self.project = Project(backend=self.backend, internal_id=a_uuid_string(), namespace=a_string(), name=a_string())

    def test_get_members(self):
        # given
        member_usernames = [a_string() for _ in range(0, 2)]
        members = [a_registered_project_member(username) for username in member_usernames]

        # and
        self.backend.get_project_members.return_value = members + [an_invited_project_member()]

        # when
        fetched_member_usernames = self.project.get_members()

        # then
        self.backend.get_project_members.assert_called_once_with(self.project.internal_id)

        # and
        self.assertEqual(member_usernames, fetched_member_usernames)

    def test_get_experiments_with_no_params(self):
        # given
        leaderboard_entries = [MagicMock() for _ in range(0, 2)]
        self.backend.get_leaderboard_entries.return_value = leaderboard_entries

        # when
        experiments = self.project.get_experiments()

        # then
        self.backend.get_leaderboard_entries.assert_called_once_with(
            project=self.project,
            ids=None,
            states=None, owners=None, tags=None,
            min_running_time=None)

        # and
        expected_experiments = [Experiment(self.backend, self.project, entry.id, entry.internal_id)
                                for entry in leaderboard_entries]
        self.assertEqual(expected_experiments, experiments)

    def test_get_experiments_with_scalar_params(self):
        # given
        leaderboard_entries = [MagicMock() for _ in range(0, 2)]
        self.backend.get_leaderboard_entries.return_value = leaderboard_entries

        # and
        params = dict(
            id=a_string(),
            state='succeeded', owner=a_string(), tag=a_string(),
            min_running_time=randint(1, 100))

        # when
        experiments = self.project.get_experiments(**params)

        # then
        expected_params = dict(
            project=self.project,
            ids=[params['id']],
            states=[params['state']], owners=[params['owner']], tags=[params['tag']],
            min_running_time=params['min_running_time']
        )
        self.backend.get_leaderboard_entries.assert_called_once_with(**expected_params)

        # and
        expected_experiments = [Experiment(self.backend, self.project, entry.id, entry.internal_id)
                                for entry in leaderboard_entries]
        self.assertEqual(expected_experiments, experiments)

    def test_get_experiments_with_list_params(self):
        # given
        leaderboard_entries = [MagicMock() for _ in range(0, 2)]
        self.backend.get_leaderboard_entries.return_value = leaderboard_entries

        # and
        params = dict(
            id=a_string_list(),
            state=['succeeded', 'failed'], owner=a_string_list(), tag=a_string_list(),
            min_running_time=randint(1, 100))

        # when
        experiments = self.project.get_experiments(**params)

        # then
        expected_params = dict(
            project=self.project,
            ids=params['id'],
            states=params['state'], owners=params['owner'], tags=params['tag'],
            min_running_time=params['min_running_time']
        )
        self.backend.get_leaderboard_entries.assert_called_once_with(**expected_params)

        # and
        expected_experiments = [Experiment(self.backend, self.project, entry.id, entry.internal_id)
                                for entry in leaderboard_entries]
        self.assertEqual(expected_experiments, experiments)

    def test_get_leaderboard(self):
        # given
        self.backend.get_leaderboard_entries.return_value = [LeaderboardEntry(some_exp_entry_dto)]

        # when
        leaderboard = self.project.get_leaderboard()

        # then
        self.backend.get_leaderboard_entries.assert_called_once_with(
            project=self.project,
            ids=None,
            states=None, owners=None, tags=None,
            min_running_time=None)

        # and
        expected_data = {0: some_exp_entry_row}
        expected_leaderboard = pd.DataFrame.from_dict(data=expected_data, orient='index')
        expected_leaderboard = expected_leaderboard.reindex(
            # pylint: disable=protected-access
            self.project._sort_leaderboard_columns(expected_leaderboard.columns), axis='columns')

        self.assertTrue(leaderboard.equals(expected_leaderboard))

    def test_sort_leaderboard_columns(self):
        # given
        columns_in_expected_order = [
            'id', 'name', 'created', 'finished', 'owner',
            'notes', 'size', 'tags',
            'channel_abc', 'channel_def',
            'parameter_abc', 'parameter_def',
            'property_abc', 'property_def'
        ]

        # when
        # pylint: disable=protected-access
        sorted_columns = self.project._sort_leaderboard_columns(reversed(columns_in_expected_order))

        # then
        self.assertEqual(columns_in_expected_order, sorted_columns)

    def test_full_id(self):
        # expect
        self.assertEqual(self.project.namespace + '/' + self.project.name, self.project.full_id)

    def test_to_string(self):
        # expect
        self.assertEqual('Project({})'.format(self.project.full_id), str(self.project))

    def test_repr(self):
        # expect
        self.assertEqual('Project({})'.format(self.project.full_id), repr(self.project))

    # pylint: disable=protected-access
    def test_get_current_experiment_from_stack(self):
        # given
        experiment = Munch(internal_id=a_uuid_string())

        # when
        self.project._push_new_experiment(experiment)

        # then
        self.assertEqual(self.project._get_current_experiment(), experiment)

    # pylint: disable=protected-access
    def test_pop_experiment_from_stack(self):
        # given
        first_experiment = Munch(internal_id=a_uuid_string())
        second_experiment = Munch(internal_id=a_uuid_string())
        # and
        self.project._push_new_experiment(first_experiment)

        # when
        self.project._push_new_experiment(second_experiment)

        # then
        self.assertEqual(self.project._get_current_experiment(), second_experiment)
        # and
        self.project._remove_stopped_experiment(second_experiment)
        # and
        self.assertEqual(self.project._get_current_experiment(), first_experiment)

    # pylint: disable=protected-access
    def test_empty_stack(self):
        # expect
        with self.assertRaises(NoExperimentContext):
            self.project._get_current_experiment()


if __name__ == '__main__':
    unittest.main()
