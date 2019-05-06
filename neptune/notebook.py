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

from neptune.utils import validate_notebook_path


class Notebook(object):
    """It contains all the information about a Neptune Notebook

    Args:
        client(`neptune.Client'): Client object
        _id(`str`): Notebook uuid
        owner(`str`):


    Examples:
        Instantiate a session and fetch a project.

        >>> import neptune
        >>> project = neptune.init()

        Create a notebook.

        >>> notebook = project.create_notebook('file.ipynb')
    """

    def __init__(self, client, _id, owner):
        self._client = client
        self._id = _id
        self._owner = owner

    @property
    def id(self):
        """ Notebook id

        Examples:
            Instantiate a session and fetch a project.

            >>> import neptune
            >>> project = neptune.init()

            Create a notebook.

            >>> notebook = project.create_notebook('file.ipynb')

            Get notebook uuid.

            >>> notebook.id
            '8ae60b26-6fe9-11e9-acf8-637723906e21'
        """
        return self._id

    @property
    def owner(self):
        return self._owner

    def add_checkpoint(self, file_path):
        validate_notebook_path(file_path)

        with open(file_path) as f:
            return self._client.create_checkpoint(self.id, os.path.abspath(file_path), f)
