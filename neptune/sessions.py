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

import logging
import re
from collections import OrderedDict

from neptune.api_exceptions import ProjectNotFound
from neptune.internal.backends.backend_factory import backend_for_project
from neptune.internal.backends.hosted_neptune_backend import HostedNeptuneBackend
from neptune.exceptions import NeptuneIncorrectProjectQualifiedNameException
from neptune.patterns import PROJECT_QUALIFIED_NAME_PATTERN
from neptune.projects import Project

_logger = logging.getLogger(__name__)


class Session(object):
    """A class for running communication with Neptune.

    In order to query Neptune experiments you need to instantiate this object first.

    Args:
        backend (:class:`~neptune.backend.Backend`, optional, default is ``None``):
            By default, Neptune client library sends logs, metrics, images, etc to Neptune servers:
            either publicly available SaaS, or an on-premises installation.

            You can pass the default backend instance explicitly to specify its parameters:

            .. code :: python3

                from neptune import Session, HostedNeptuneBackend
                session = Session(backend=HostedNeptuneBackend(...))

            Passing an instance of :class:`~neptune.OfflineBackend` makes your code run without communicating
            with Neptune servers.

            .. code :: python3

                from neptune import Session, OfflineBackend
                session = Session(backend=OfflineBackend())

        api_token (:obj:`str`, optional, default is ``None``):
            User's API token. If ``None``, the value of ``NEPTUNE_API_TOKEN`` environment variable will be taken.
            Parameter is ignored if ``backend`` is passed.

            .. deprecated :: 0.4.4

            Instead, use:

            .. code :: python3

                from neptune import Session
                session = Session.with_default_backend(api_token='...')

        proxies (:obj:`str`, optional, default is ``None``):
            Argument passed to HTTP calls made via the `Requests <https://2.python-requests.org/en/master/>`_ library.
            For more information see their proxies
            `section <https://2.python-requests.org/en/master/user/advanced/#proxies>`_.
            Parameter is ignored if ``backend`` is passed.

            .. deprecated :: 0.4.4

            Instead, use:

            .. code :: python3

                from neptune import Session, HostedNeptuneBackend
                session = Session(backend=HostedNeptuneBackend(proxies=...))

    Examples:

        Create session, assuming you have created an environment variable ``NEPTUNE_API_TOKEN``

        .. code:: python3

            from neptune import Session
            session = Session.with_default_backend()

        Create session and pass ``api_token``

        .. code:: python3

            from neptune import Session
            session = Session.with_default_backend(api_token='...')

        Create an offline session

        .. code:: python3

            from neptune import Session, OfflineBackend
            session = Session(backend=OfflineBackend())

    """
    def __init__(self, api_token=None, proxies=None, backend=None):
        self._backend = backend

        if self._backend is None:
            _logger.warning('WARNING: Instantiating Session without specifying a backend is deprecated '
                            'and will be removed in future versions. For current behaviour '
                            'use `neptune.init(...)` or `Session.with_default_backend(...)')

            self._backend = HostedNeptuneBackend(api_token, proxies)

    @classmethod
    def with_default_backend(cls, api_token=None):
        """The simplest way to instantiate a ``Session``.

        Args:
            api_token (:obj:`str`):
                User's API token.
                If ``None``, the value of ``NEPTUNE_API_TOKEN`` environment variable will be taken.

        Examples:

            .. code :: python3

                from neptune import Session
                session = Session.with_default_backend()

        """
        return cls(backend=HostedNeptuneBackend(api_token))

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
            raise NeptuneIncorrectProjectQualifiedNameException(project_qualified_name)

        return self._backend.get_project(project_qualified_name)

    def get_projects(self, namespace):
        """Get all projects that you have permissions to see in given workspace.

        | This method gets you all available projects names and their
          corresponding :class:`~neptune.projects.Project` objects.
        | Both private and public projects may be returned for the workspace.
          If you have role in private project, it is included.
        | You can retrieve all the public projects that belong to any user or workspace,
          as long as you know their username or workspace name.

        Args:
            namespace (:obj:`str`): It can either be name of the workspace or username.

        Returns:
            :obj:`OrderedDict`
                | **keys** are ``project_qualified_name`` that is: *'workspace/project_name'*
                | **values** are corresponding :class:`~neptune.projects.Project` objects.

        Raises:
            `NamespaceNotFound`: When the given namespace does not exist.

        Examples:

            .. code:: python3

                # create Session
                from neptune.sessions import Session
                session = Session()

                # Now, you can list all the projects available for a selected namespace.
                # You can use `YOUR_NAMESPACE` which is your workspace or user name.
                # You can also list public projects created in other workspaces.
                # For example you can use the `neptune-ai` namespace.

                session.get_projects('neptune-ai')

                # Example output:
                # OrderedDict([('neptune-ai/credit-default-prediction',
                #               Project(neptune-ai/credit-default-prediction)),
                #              ('neptune-ai/GStore-Customer-Revenue-Prediction',
                #               Project(neptune-ai/GStore-Customer-Revenue-Prediction)),
                #              ('neptune-ai/human-protein-atlas',
                #               Project(neptune-ai/human-protein-atlas)),
                #              ('neptune-ai/Ships',
                #               Project(neptune-ai/Ships)),
                #              ('neptune-ai/Mapping-Challenge',
                #               Project(neptune-ai/Mapping-Challenge))
                #              ])
        """
        old_backend = self._backend
        new_backend = self._backend.alpha_backend
        projects = [
            Project(
                backend_for_project(api_project=p, old_backend=old_backend, new_backend=new_backend),
                p.id,
                namespace,
                p.name
            ) for p in self._backend.get_projects(namespace)
        ]
        return OrderedDict((p.full_id, p) for p in projects)
