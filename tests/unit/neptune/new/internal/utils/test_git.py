#
# Copyright (c) 2023, Neptune Labs Sp. z o.o.
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
import datetime

from mock import (
    MagicMock,
    patch,
)

from neptune.internal.utils.git import (
    DiffTracker,
    GitInfo,
    to_git_info,
)
from neptune.types import GitRef


class TestGit:
    def test_disabled(self):
        assert to_git_info(GitRef.DISABLED) is None

    @patch("git.Repo", return_value=MagicMock())
    def test_getting_git_info(self, repo_mock):
        # given
        now = datetime.datetime.now()
        repo = repo_mock.return_value
        repo.is_dirty.return_value = True
        repo.head.commit.hexsha = "sha"
        repo.head.commit.message = "message"
        repo.head.commit.author.name = "author_name"
        repo.head.commit.author.email = "author@email"
        repo.head.commit.committed_datetime = now
        repo.active_branch.name = "master"
        repo.remotes = []

        # when
        git_info = to_git_info(GitRef("."))

        # then
        assert git_info == GitInfo(
            commit_id="sha",
            message="message",
            author_name="author_name",
            author_email="author@email",
            commit_date=now,
            dirty=True,
            branch="master",
            remotes=[],
        )


class TestDiffTracker:
    def test_get_head_index_diff(self):
        repo_mock = MagicMock()
        repo_mock.git.diff.return_value = "some_diff"
        repo_mock.head.name = "HEAD"

        tracker = DiffTracker(repo_mock)

        diff = tracker.get_head_index_diff()

        repo_mock.git.diff.assert_called_once_with("HEAD")
        assert diff == "some_diff"

    def test_upstream_index_diff_tracking_branch_present(self):
        repo_mock = MagicMock()
        repo_mock.git.diff.return_value = "some_diff"
        tracking_branch = MagicMock()
        tracking_branch.commit.hexsha = "sha1234"
        repo_mock.active_branch.tracking_branch.return_value = tracking_branch

        tracker = DiffTracker(repo_mock)

        diff = tracker.get_upstream_index_diff()

        repo_mock.git.diff.assert_called_once_with("sha1234")
        assert diff == "some_diff"
        assert tracker.upstream_commit_sha == "sha1234"

    def test_upstream_index_diff_tracking_branch_not_present(self):
        repo_mock = MagicMock()
        repo_mock.git.diff.return_value = "some_diff"
        repo_mock.active_branch.tracking_branch.return_value = None

        tracking_branch = MagicMock()
        branch = MagicMock()
        branch.tracking_branch.return_value = tracking_branch
        repo_mock.branches = [branch, branch, branch]

        repo_mock.is_ancestor.return_value = True
        ancestor = MagicMock()
        ancestor.hexsha = "sha1234"
        repo_mock.merge_base.return_value = [ancestor, ancestor]

        tracker = DiffTracker(repo_mock)

        diff = tracker.get_upstream_index_diff()

        assert diff == "some_diff"
        assert tracker.upstream_commit_sha == "sha1234"

        repo_mock.git.diff.assert_called_once_with("sha1234")
        repo_mock.active_branch.tracking_branch.assert_called_once()

        assert repo_mock.merge_base.call_count == 3
        assert repo_mock.is_ancestor.call_count == 5  # 6 ancestors - 1 case when most_recent_ancestor was None

    def test_detached_head(self):
        repo_mock = MagicMock()
        repo_mock.active_branch.tracking_branch.side_effect = TypeError
        repo_mock.git.diff = MagicMock()

        tracker = DiffTracker(repo_mock)
        diff = tracker.get_upstream_index_diff()

        assert diff is None
        assert tracker.upstream_commit_sha is None
        repo_mock.git.diff.assert_not_called()
