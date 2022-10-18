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

from neptune.common.utils import validate_notebook_path
from neptune.legacy.internal.utils.deprecation import legacy_client_deprecation


class Notebook(object):
    """It contains all the information about a Neptune Notebook

    Args:
        backend (:class:`~neptune.ApiClient`): A ApiClient object
        project (:class:`~neptune.projects.Project`): Project object
        _id (:obj:`str`): Notebook id
        owner (:obj:`str`): Creator of the notebook is the Notebook owner

    Examples:
        .. code:: python3

            # Create a notebook in Neptune.
            notebook = project.create_notebook('data_exploration.ipynb')

    """

    @legacy_client_deprecation
    def __init__(self, backend, project, _id, owner):
        self._backend = backend
        self._project = project
        self._id = _id
        self._owner = owner

    @property
    def id(self):
        return self._id

    @property
    def owner(self):
        return self._owner

    def add_checkpoint(self, file_path):
        """Uploads new checkpoint of the notebook to Neptune

        Args:
            file_path (:obj:`str`): File path containing notebook contents

        Example:

            .. code:: python3

                # Create a notebook.
                notebook = project.create_notebook('file.ipynb')

                # Change content in your notebook & save it

                # Upload new checkpoint
                notebook.add_checkpoint('file.ipynb')
        """
        validate_notebook_path(file_path)

        with open(file_path) as f:
            return self._backend.create_checkpoint(self.id, os.path.abspath(file_path), f)

    def get_path(self):
        """Returns the path used to upload the current checkpoint of this notebook

        Returns:
            :obj:`str`: path of the current checkpoint
        """
        return self._backend.get_last_checkpoint(self._project, self._id).path

    def get_name(self):
        """Returns the name used to upload the current checkpoint of this notebook

        Returns:
            :obj:`str`: the name of current checkpoint
        """
        return self._backend.get_last_checkpoint(self._project, self._id).name
