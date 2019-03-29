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

import os
import sys
import subprocess

from datetime import datetime


class GitInfo(object):
    def __init__(self, commit_id, message, author_name, author_email, commit_date, repository_dirty):
        self.commit_id = commit_id
        self.message = message
        self.author_name = author_name
        self.author_email = author_email
        self.commit_date = commit_date
        self.repository_dirty = repository_dirty


def get_git_info():
    try:
        from dulwich.repo import Repo  # pylint:disable=wrong-import-position
        from dulwich import porcelain  # pylint:disable=wrong-import-position

        def get_git_version():
            try:
                with open(os.devnull, 'w') as devnull:
                    return subprocess.check_output(['git', '--version'], stderr=devnull).decode("utf-8").strip()
            except:  # pylint: disable=bare-except
                return None

        if not get_git_version():
            return None

        if hasattr(sys, 'getwindowsversion') and r'GIT_PYTHON_GIT_EXECUTABLE' not in os.environ:
            os.environ[r'GIT_PYTHON_GIT_EXECUTABLE'] = os.popen("where git").read().strip()

        repo = Repo.discover()
        commit = repo[repo.head()]

        status = porcelain.status()
        dirty = bool([entry for k in status.staged for entry in status.staged[k]]
                     + [entry for entry in status.unstaged])

        author = commit.author
        author_name = b''
        author_email = b''
        if author:
            split = author.split(b'<')
            if len(split) == 2:
                author_name = split[0]
                author_email = split[1][:-1]

        return GitInfo(
            commit_id=commit.sha().hexdigest(),
            message=commit.message,
            author_name=author_name,
            author_email=author_email,
            commit_date=datetime.fromtimestamp(commit.commit_time + commit.commit_timezone),
            repository_dirty=dirty
        )

    except:  # pylint: disable=bare-except
        return None
