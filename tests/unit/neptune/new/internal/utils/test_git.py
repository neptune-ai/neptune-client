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

import git
from git import Repo
from mock import (
    MagicMock,
    patch,
)

from neptune.internal.utils.git import (
    GitInfo,
    get_diff,
    get_relevant_upstream_commit,
    get_repo_from_git_ref,
    get_uncommitted_changes,
    get_upstream_index_sha,
    search_for_most_recent_ancestor,
    to_git_info,
    track_uncommitted_changes,
)
from neptune.types import GitRef


class TestGit:
    def test_disabled(self):
        assert to_git_info(GitRef.DISABLED) is None

    @patch("git.Repo")
    def test_getting_git_info(self, mock_repo):
        # given
        now = datetime.datetime.now()
        repo = mock_repo.return_value
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


def test_get_repo_from_git_ref_disabled():
    # given
    git_ref = GitRef.DISABLED

    # when
    repo = get_repo_from_git_ref(git_ref)

    # then
    assert repo is None


def test_get_repo_from_git_ref():
    # given
    git_ref = GitRef()

    # when
    repo = get_repo_from_git_ref(git_ref)

    # then
    assert isinstance(repo, git.Repo)


@patch("git.Repo")
def test_get_diff(mock_repo):
    # when
    get_diff(mock_repo, "some_ref")

    # then
    mock_repo.git.diff.assert_called_once_with("some_ref", index=False)


@patch("git.Repo")
def test_get_diff_command_error(mock_repo):
    # given
    mock_repo.git.diff.side_effect = git.GitCommandError("diff")

    # when
    diff = get_diff(mock_repo, "some_ref")

    # then
    mock_repo.git.diff.assert_called_once_with("some_ref", index=False)
    assert diff is None


@patch("git.Repo")
def test_search_for_most_recent_ancestor(mock_repo):
    # given
    mock_repo.active_branch.tracking_branch.return_value = None

    tracking_branch = MagicMock()
    branch = MagicMock()
    branch.tracking_branch.return_value = tracking_branch
    mock_repo.heads = [branch, branch, branch]

    mock_repo.is_ancestor.return_value = True
    ancestor = MagicMock()
    ancestor_to_be_chosen = MagicMock()
    ancestor_to_be_chosen.hexsha = "sha1234"
    mock_repo.merge_base.return_value = [ancestor, ancestor_to_be_chosen]

    # when
    searched_ancestor = search_for_most_recent_ancestor(mock_repo)

    # then
    assert searched_ancestor.hexsha == "sha1234"

    assert mock_repo.merge_base.call_count == 3
    assert mock_repo.is_ancestor.call_count == 5  # 6 ancestors - 1 case when most_recent_ancestor was None


@patch("neptune.internal.utils.git.search_for_most_recent_ancestor")
@patch("git.Repo")
def test_get_relevant_upstream_commit_no_search(mock_repo, mock_search):
    # when
    upstream_commit = get_relevant_upstream_commit(mock_repo)

    # then
    assert upstream_commit == mock_repo.active_branch.tracking_branch.return_value.commit
    mock_search.assert_not_called()


@patch("neptune.internal.utils.git.search_for_most_recent_ancestor")
@patch("git.Repo")
def test_get_relevant_upstream_commit_with_search(mock_repo, mock_search):
    # given
    mock_repo.active_branch.tracking_branch.return_value = None

    # when
    upstream_commit = get_relevant_upstream_commit(mock_repo)

    # then
    assert upstream_commit == mock_search.return_value
    mock_search.assert_called_once_with(mock_repo)


@patch("neptune.internal.utils.git.get_relevant_upstream_commit")
@patch("git.Repo")
def test_get_upstream_index_sha(mock_repo, mock_get_upstream_commit):
    # given
    mock_get_upstream_commit.return_value.hexsha = "test_sha"

    # when
    sha = get_upstream_index_sha(mock_repo)

    # then
    assert sha == "test_sha"
    mock_get_upstream_commit.assert_called_once_with(mock_repo)


@patch("git.Repo")
def test_detached_head(mock_repo):
    # given
    mock_repo.active_branch.tracking_branch.side_effect = TypeError

    # when
    sha = get_upstream_index_sha(mock_repo)

    # then
    assert sha is None
    mock_repo.git.diff.assert_not_called()


@patch("git.Repo")
@patch("neptune.internal.utils.git.get_upstream_index_sha", return_value="test_sha")
def test_get_uncommitted_changes(mock_get_sha, mock_repo):
    # given
    mock_repo.git.diff.return_value = "some_diff"
    mock_repo.head.name = "HEAD"

    # when
    uncommitted_changes = get_uncommitted_changes(mock_repo)

    # then
    assert mock_repo.git.diff.call_count == 2
    assert mock_get_sha.call_count == 1
    assert uncommitted_changes.diff_head == "some_diff\n"
    assert uncommitted_changes.upstream_sha == "test_sha"
    assert uncommitted_changes.diff_upstream == "some_diff\n"


@patch("git.Repo")
def test_get_uncommitted_changes_clean_repo(tmp_path_factory):
    # given
    path = tmp_path_factory.mktemp("git_repo")
    repo = Repo.init(path)

    # when
    uncommitted_changes = get_uncommitted_changes(repo)

    # then
    assert uncommitted_changes is None


@patch("neptune.internal.utils.git.get_uncommitted_changes")
@patch("neptune.metadata_containers.Run")
def test_git_ref_disabled(mock_run, mock_get_changes):
    # when
    track_uncommitted_changes(GitRef.DISABLED, mock_run)

    # then
    mock_get_changes.assert_not_called()


@patch("neptune.internal.utils.git.get_uncommitted_changes")
@patch("neptune.internal.utils.git.get_repo_from_git_ref")
@patch("neptune.internal.utils.git.File")
@patch("neptune.metadata_containers.Run")
def test_track_uncommitted_changes(mock_run, mock_file, mock_get_repo, mock_get_changes):
    # given
    git_ref = GitRef()

    # when
    track_uncommitted_changes(git_ref, mock_run)

    # then
    assert mock_file.from_content.call_count == 2
    mock_get_repo.assert_called_once_with(git_ref)
    mock_get_changes.assert_called_once()
