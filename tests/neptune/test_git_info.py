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

import datetime
import mock

from neptune.git_info import GitInfo
from neptune.utils import get_git_info


class TestGitInfo(unittest.TestCase):

    def test_getting_some_git_info_from_current_repository(self):
        # when
        git_info = get_git_info('.')

        # then
        self.assertNotEqual(git_info, None)

    @mock.patch('git.Repo', return_value=mock.MagicMock())
    def test_getting_git_info(self, repo_mock):
        # given
        now = datetime.datetime.now()
        repo = repo_mock.return_value
        repo.is_dirty.return_value = True
        repo.head.commit.hexsha = 'sha'
        repo.head.commit.message = 'message'
        repo.head.commit.author.name = 'author_name'
        repo.head.commit.author.email = 'author@email'
        repo.head.commit.committed_datetime = now
        repo.active_branch.name = "master"
        repo.remotes = []

        # when
        git_info = get_git_info('.')

        # then
        self.assertEqual(git_info, GitInfo(
            commit_id='sha',
            message='message',
            author_name='author_name',
            author_email='author@email',
            commit_date=now,
            repository_dirty=True,
            active_branch="master",
            remote_urls=[]
        ))


if __name__ == '__main__':
    unittest.main()
