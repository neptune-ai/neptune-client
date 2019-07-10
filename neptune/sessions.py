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
import re
from collections import OrderedDict

from neptune.api_exceptions import ProjectNotFound
from neptune.client import Client
from neptune.credentials import Credentials
from neptune.exceptions import IncorrectProjectQualifiedName
from neptune.patterns import PROJECT_QUALIFIED_NAME_PATTERN
from neptune.projects import Project


class Session(object):
    """A class for running communication with Neptune.

    In order to query Neptune experiments you need to instantiate this object first.

    Args:
        api_token (:obj:`str`, optional, default is ``None``):
            User's API token.
            If ``None``, the value of ``NEPTUNE_API_TOKEN`` environment variable will be taken.
        proxies (:obj:`str`, optional, default is ``None``):
            Argument passed to HTTP calls made via the `Requests <https://2.python-requests.org/en/master/>`_ library.
            For more information see their proxies
            `section <https://2.python-requests.org/en/master/user/advanced/#proxies>`_.

    Examples:
        Create session and pass 'api_token'

        .. code:: python3

            from neptune.sessions import Session
            session = Session(api_token='YOUR_NEPTUNE_API_TOKEN')

        Create session, assuming you have created an environment variable 'NEPTUNE_API_TOKEN'

        .. code:: python3

            from neptune.sessions import Session
            session = Session()

    """
    def __init__(self, api_token=None, proxies=None):
        credentials = Credentials(api_token)

        self.credentials = credentials
        self.proxies = proxies

        self._client = Client(self.credentials.api_address, self.credentials.api_token, proxies)

    def get_project(self, project_qualified_name):
        """Get a project with given ``project_qualified_name``.

        In order to access experiments data one needs to get a :class:`~neptune.projects.Project` object first.
        This method gives you the ability to do that.

        Args:
            project_qualified_name (:obj:`str`):
                Qualified name of a project in a form of ``namespace/project_name``.

        Returns:
            :class:`~neptune.projects.Project` object.

        Raise:
            :class:`~neptune.api_exceptions.ProjectNotFound`: When a project with given name does not exist.

        Examples:

            .. code:: python3

                # Create a Session instance
                from neptune.sessions import Session
                session = Session()

                # Get a project by it's ``project_qualified_name``:
                my_project = session.get_project('namespace/project_name')

        """
        if not project_qualified_name:
            raise ProjectNotFound(project_qualified_name)

        if not re.match(PROJECT_QUALIFIED_NAME_PATTERN, project_qualified_name):
            raise IncorrectProjectQualifiedName(project_qualified_name)

        project = self._client.get_project(project_qualified_name)
        return Project(
            client=self._client,
            internal_id=project.id,
            namespace=project.organizationName,
            name=project.name
        )

    def get_projects(self, namespace):
        """It gets all project and full project names for given namespace

        In order to access experiment data one needs to get a `Project` object first. This method helps you figure out
        what are the available projects and access the project of interest.
        You can list both your private and public projects.
        You can also access all the public projects that belong to any user or organization,
        as long as you know what is their namespace.

        Args:
            namespace(str): It can either be your organization or user name. You can list all the public projects
                for any organization or user you want as long as you know their namespace.

        Returns:
            dict: Dictionary of "NAMESPACE/PROJECT_NAME" and `neptune.project.Project` object pairs that contains
            all the projects that belong to the selected namespace.

        Raises:
            `NamespaceNotFound`: When the given namespace does not exist.

        Examples:
            First, you need to create a Session instance:

            >>> from neptune.sessions import Session
            >>> session = Session()

            Now, you can list all the projects available for a selected namespace. You can use `YOUR_NAMESPACE` which
            is your organization or user name. You can also list public projects created by other organizations.
            For example you can use the `neptune-ml` namespace.

            >>> session.get_projects('neptune-ml')
            {'neptune-ml/Sandbox': Project(neptune-ml/Sandbox),
            'neptune-ml/Home-Credit-Default-Risk': Project(neptune-ml/Home-Credit-Default-Risk),
            'neptune-ml/Mapping-Challenge': Project(neptune-ml/Mapping-Challenge),
            'neptune-ml/Ships': Project(neptune-ml/Ships),
            'neptune-ml/human-protein-atlas': Project(neptune-ml/human-protein-atlas),
            'neptune-ml/Salt-Detection': Project(neptune-ml/Salt-Detection),
            'neptune-ml/Data-Science-Bowl-2018': Project(neptune-ml/Data-Science-Bowl-2018)}
        """

        projects = [Project(self._client, p.id, namespace, p.name) for p in self._client.get_projects(namespace)]
        return OrderedDict((p.full_id, p) for p in projects)
