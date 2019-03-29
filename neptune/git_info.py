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


class GitInfo(object):
    """ Holds information about a git repository"""
    def __init__(self,
                 commit_id,
                 message="",
                 author_name="",
                 author_email="",
                 commit_date="",
                 repository_dirty=True):
        """Creates a new instance of a git repository info

        Args:
            commit_id(str): commit id sha
            message(str): commit message
            author_name(str): commit author username
            author_email(str): commit author email
            commit_date(datetime.datetime): commit datetime
            repository_dirty(bool): True, if the repository has uncommitted changes
        """
        if commit_id is None:
            raise TypeError("commit_id must not be None")

        self.commit_id = commit_id
        self.message = message
        self.author_name = author_name
        self.author_email = author_email
        self.commit_date = commit_date
        self.repository_dirty = repository_dirty

    def __eq__(self, o):
        return o is not None and self.__dict__ == o.__dict__

    def __ne__(self, o):
        return not self.__eq__(o)

    def __str__(self):
        return 'GitInfo({})'.format(self.commit_id)

    def __repr__(self):
        return str(self)
