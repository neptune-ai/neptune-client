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
import threading

from neptune import envs, projects, experiments
from neptune.exceptions import MissingProjectQualifiedName, Uninitialized
from neptune.sessions import Session
from ._version import get_versions

__version__ = get_versions()['version']
del get_versions

session = None
project = None

__lock = threading.RLock()


def init(project_qualified_name=None, api_token=None, proxies=None):
    """Initialize `Neptune client library <https://github.com/neptune-ml/neptune-client>`_ to work with
    specific project.

    Authorize user, sets value of global variable ``project`` to :class:`~neptune.projects.Project` object
    that can be use to create or list experiments and notebooks, etc.

    Args:
        project_qualified_name (:obj:`str`, optional, default is ``None``):
            Qualified name of a project in a form of ``namespace/project_name``.
            If ``None``, the value of ``NEPTUNE_PROJECT`` environment variable will be taken.

        api_token (:obj:`str`, optional, default is ``None``):
            User's API token.
            If ``None``, the value of ``NEPTUNE_API_TOKEN`` environment variable will be taken.

        proxies (:obj:`str`, optional, default is ``None``):

    Note:
        It is strongly recommended to use ``NEPTUNE_API_TOKEN`` environment variable rather than
        placing your API token in plain text in your source code.

    Returns:
        :class:`~neptune.projects.Project` object that is used to create or list experiments and notebooks, etc.

    Raises:
        `MissingApiToken`: When ``api_token`` is None
            and ``NEPTUNE_API_TOKEN`` environment variable was not set.

        `MissingProjectQualifiedName`: When ``project_qualified_name`` is None and ``NEPTUNE_PROJECT``
            environment variable was not set.

        `InvalidApiKey`: When given ``api_token`` is malformed.

        `Unauthorized`: When given ``api_token`` is invalid.

    Examples:

        .. code:: python3

            # minimal invoke
            neptune.init()

            # specifying project name
            neptune.init('jack/sandbox')
    """
    # TODO: Document `proxies` argument.

    if project_qualified_name is None:
        project_qualified_name = os.getenv(envs.PROJECT_ENV_NAME)

    # pylint: disable=global-statement
    with __lock:
        global session, project

        session = Session(api_token=api_token, proxies=proxies)

        if project_qualified_name is None:
            raise MissingProjectQualifiedName()

        project = session.get_project(project_qualified_name)

        return project


def set_project(project_qualified_name):
    """Setups `Neptune client library <https://github.com/neptune-ml/neptune-client>`_ to work with specific project.

    | Sets value of global variable ``project`` to :class:`~neptune.projects.Project` object
      that can be use to create or list experiments and notebooks, etc.
    | If Neptune client library was not previously initialized via :meth:`~neptune.init` call
      it will be initialized with API token taken from ``NEPTUNE_API_TOKEN`` environment variable.

    Args:
        project_qualified_name (:obj:`str`):
            Qualified name of a project in a form of ``namespace/project_name``.

    Returns:
        :class:`~neptune.projects.Project` object that is used to create or list experiments and notebooks, etc.

    Raises:
        `MissingApiToken`: When library was not initialized previously by ``init`` call and
            ``NEPTUNE_API_TOKEN`` environment variable is not set

    Examples:

        .. code:: python3

            # minimal invoke
            neptune.set_project('jack/sandbox')
    """

    # pylint: disable=global-statement
    with __lock:
        global session, project

        if session is None:
            session = init(project_qualified_name=project_qualified_name)
        else:
            project = session.get_project(project_qualified_name)

        return project


def create_experiment(name=None,
                      description=None,
                      params=None,
                      properties=None,
                      tags=None,
                      upload_source_files=None,
                      abort_callback=None,
                      logger=None,
                      upload_stdout=True,
                      upload_stderr=True,
                      send_hardware_metrics=True,
                      run_monitoring_thread=True,
                      handle_uncaught_exceptions=True,
                      git_info=None,
                      hostname=None,
                      notebook_id=None):
    # pylint: disable=global-statement
    global project
    if project is None:
        raise Uninitialized()

    return project.create_experiment(
        name=name,
        description=description,
        params=params,
        properties=properties,
        tags=tags,
        upload_source_files=upload_source_files,
        abort_callback=abort_callback,
        logger=logger,
        upload_stdout=upload_stdout,
        upload_stderr=upload_stderr,
        send_hardware_metrics=send_hardware_metrics,
        run_monitoring_thread=run_monitoring_thread,
        handle_uncaught_exceptions=handle_uncaught_exceptions,
        git_info=git_info,
        hostname=hostname,
        notebook_id=notebook_id
    )


get_experiment = experiments.get_current_experiment


def append_tag(tag):
    get_experiment().append_tag(tag)


def remove_tag(tag):
    get_experiment().remove_tag(tag)


def set_property(key, value):
    get_experiment().set_property(key, value)


def remove_property(key):
    get_experiment().remove_property(key)


def send_metric(channel_name, x, y=None, timestamp=None):
    return get_experiment().send_metric(channel_name, x, y, timestamp)


def send_text(channel_name, x, y=None, timestamp=None):
    return get_experiment().send_text(channel_name, x, y, timestamp)


def send_image(channel_name, x, y=None, name=None, description=None, timestamp=None):
    return get_experiment().send_image(channel_name, x, y, name, description, timestamp)


def send_artifact(artifact):
    return get_experiment().send_artifact(artifact)


def stop(traceback=None):
    get_experiment().stop(traceback)
