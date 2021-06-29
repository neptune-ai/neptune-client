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
import os
import threading

from neptune import constants, envs
from neptune._version import get_versions
from neptune.exceptions import (
    InvalidNeptuneBackend,
    NeptuneUninitializedException,
    NeptuneIncorrectImportException,
)
from neptune.internal.api_clients import backend_factory
from neptune.internal.api_clients.offline_backend import OfflineBackend
from neptune.internal.backends.hosted_neptune_backend import HostedNeptuneBackend
from neptune.projects import Project
from neptune.sessions import Session
from neptune.utils import assure_project_qualified_name

__version__ = get_versions()['version']

del get_versions

session = None
project = None

__lock = threading.RLock()

_logger = logging.getLogger(__name__)

"""Access Neptune as an anonymous user.
You can pass this value as api_token during init() call, either by an environment variable or passing it directly
"""
ANONYMOUS = constants.ANONYMOUS

"""Anonymous user API token.
You can pass this value as api_token during init() call, either by an environment variable or passing it directly
"""
ANONYMOUS_API_TOKEN = constants.ANONYMOUS_API_TOKEN


CURRENT_KWARGS = (
    'project', 'run', 'custom_run_id', 'mode', 'name', 'description', 'tags',
    'source_files', 'capture_stdout', 'capture_stderr', 'capture_hardware_metrics',
    'fail_on_exception', 'monitoring_namespace', 'flush_period',
)


def _check_for_extra_kwargs(caller_name, kwargs: dict):
    for name in CURRENT_KWARGS:
        if name in kwargs:
            raise NeptuneIncorrectImportException()
    if kwargs:
        first_key = next(iter(kwargs.keys()))
        raise TypeError(f"{caller_name}() got an unexpected keyword argument '{first_key}'")


def init(project_qualified_name=None, api_token=None, proxies=None, backend=None, **kwargs):
    """Initialize `Neptune client library <https://github.com/neptune-ai/neptune-client>`_ to work with
    specific project.

    Authorize user, sets value of global variable ``project`` to :class:`~neptune.projects.Project` object
    that can be used to create or list experiments, notebooks, etc.

    Args:
        project_qualified_name (:obj:`str`, optional, default is ``None``):
            Qualified name of a project in a form of ``namespace/project_name``.
            If ``None``, the value of ``NEPTUNE_PROJECT`` environment variable will be taken.

        api_token (:obj:`str`, optional, default is ``None``):
            User's API token. If ``None``, the value of ``NEPTUNE_API_TOKEN`` environment variable will be taken.

            .. note::

                It is strongly recommended to use ``NEPTUNE_API_TOKEN`` environment variable rather than
                placing your API token in plain text in your source code.

        proxies (:obj:`dict`, optional, default is ``None``):
            Argument passed to HTTP calls made via the `Requests <https://2.python-requests.org/en/master/>`_ library.
            For more information see their proxies
            `section <https://2.python-requests.org/en/master/user/advanced/#proxies>`_.

            .. note::

                Only `http` and `https` keys are supported by all features.

            .. deprecated :: 0.4.4

            Instead, use:

            .. code :: python3

                from neptune import HostedNeptuneBackendApiClient
                neptune.init(backend=HostedNeptuneBackendApiClient(proxies=...))

        backend (:class:`~neptune.ApiClient`, optional, default is ``None``):
            By default, Neptune client library sends logs, metrics, images, etc to Neptune servers:
            either publicly available SaaS, or an on-premises installation.

            You can also pass the default backend instance explicitly to specify its parameters:

            .. code :: python3

                from neptune import HostedNeptuneBackendApiClient
                neptune.init(backend=HostedNeptuneBackendApiClient(...))

            Passing an instance of :class:`~neptune.OfflineApiClient` makes your code run without communicating
            with Neptune servers.

            .. code :: python3

                from neptune import OfflineApiClient
                neptune.init(backend=OfflineApiClient())

            .. note::
                Instead of passing a ``neptune.OfflineApiClient`` instance as ``backend``, you can set an
                environment variable ``NEPTUNE_BACKEND=offline`` to override the default behaviour.

    Returns:
        :class:`~neptune.projects.Project` object that is used to create or list experiments, notebooks, etc.

    Raises:
        `NeptuneMissingApiTokenException`: When ``api_token`` is None
            and ``NEPTUNE_API_TOKEN`` environment variable was not set.
        `NeptuneMissingProjectQualifiedNameException`: When ``project_qualified_name`` is None
            and ``NEPTUNE_PROJECT`` environment variable was not set.
        `InvalidApiKey`: When given ``api_token`` is malformed.
        `Unauthorized`: When given ``api_token`` is invalid.

    Examples:

        .. code:: python3

            # minimal invoke
            neptune.init()

            # specifying project name
            neptune.init('jack/sandbox')

            # running offline
            neptune.init(backend=neptune.OfflineApiClient())
    """

    _check_for_extra_kwargs(init.__name__, kwargs)
    project_qualified_name = assure_project_qualified_name(project_qualified_name)

    # pylint: disable=global-statement
    with __lock:
        global session, project

        if backend is None:
            backend_name = os.getenv(envs.BACKEND)
            backend = backend_factory(
                backend_name=backend_name,
                api_token=api_token,
                proxies=proxies,
            )

        session = Session(backend=backend)
        project = session.get_project(project_qualified_name)

        return project


def set_project(project_qualified_name):
    """Setups `Neptune client library <https://github.com/neptune-ai/neptune-client>`_ to work with specific project.

    | Sets value of global variable ``project`` to :class:`~neptune.projects.Project` object
      that can be used to create or list experiments, notebooks, etc.
    | If Neptune client library was not previously initialized via :meth:`~neptune.init` call
      it will be initialized with API token taken from ``NEPTUNE_API_TOKEN`` environment variable.

    Args:
        project_qualified_name (:obj:`str`):
            Qualified name of a project in a form of ``namespace/project_name``.

    Returns:
        :class:`~neptune.projects.Project` object that is used to create or list experiments, notebooks, etc.

    Raises:
        `NeptuneMissingApiTokenException`: When library was not initialized previously by ``init`` call and
            ``NEPTUNE_API_TOKEN`` environment variable is not set.

    Examples:

        .. code:: python3

            # minimal invoke
            neptune.set_project('jack/sandbox')
    """

    # pylint: disable=global-statement
    with __lock:
        global session, project

        if session is None:
            init(project_qualified_name=project_qualified_name)
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
    """Create and start Neptune experiment.

    Alias for: :meth:`~neptune.projects.Project.create_experiment`
    """

    # pylint: disable=global-statement
    global project
    if project is None:
        raise NeptuneUninitializedException()

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


def get_experiment():
    # pylint: disable=global-statement
    global project
    if project is None:
        raise NeptuneUninitializedException()

    # pylint: disable=protected-access
    return project._get_current_experiment()


def append_tag(tag, *tags):
    """Append tag(s) to the experiment on the top of experiments view.

    Alias for: :meth:`~neptune.experiments.Experiment.append_tag`
    """
    get_experiment().append_tag(tag, *tags)


def append_tags(tag, *tags):
    """Append tag(s) to the experiment on the top of experiments view.

    Alias for: :meth:`~neptune.experiments.Experiment.append_tags`
    """
    get_experiment().append_tag(tag, *tags)


def remove_tag(tag):
    """Removes single tag from experiment.

    Alias for: :meth:`~neptune.experiments.Experiment.remove_tag`
    """
    get_experiment().remove_tag(tag)


def set_property(key, value):
    """Set `key-value` pair as an experiment property.

    If property with given ``key`` does not exist, it adds a new one.

    Alias for: :meth:`~neptune.experiments.Experiment.set_property`
    """
    get_experiment().set_property(key, value)


def remove_property(key):
    """Removes a property with given key.

    Alias for: :meth:`~neptune.experiments.Experiment.remove_property`
    """
    get_experiment().remove_property(key)


def send_metric(channel_name, x, y=None, timestamp=None):
    """Log metrics (numeric values) in Neptune.

    Alias for :meth:`~neptune.experiments.Experiment.log_metric`
    """
    return get_experiment().send_metric(channel_name, x, y, timestamp)


def log_metric(log_name, x, y=None, timestamp=None):
    """Log metrics (numeric values) in Neptune.

    Alias for :meth:`~neptune.experiments.Experiment.log_metric`
    """
    return get_experiment().log_metric(log_name, x, y, timestamp)


def send_text(channel_name, x, y=None, timestamp=None):
    """Log text data in Neptune.

    Alias for :meth:`~neptune.experiments.Experiment.log_text`
    """
    return get_experiment().send_text(channel_name, x, y, timestamp)


def log_text(log_name, x, y=None, timestamp=None):
    """Log text data in Neptune.

    Alias for :meth:`~neptune.experiments.Experiment.log_text`
    """
    return get_experiment().send_text(log_name, x, y, timestamp)


def send_image(channel_name, x, y=None, name=None, description=None, timestamp=None):
    """Log image data in Neptune.

    Alias for :meth:`~neptune.experiments.Experiment.log_image`
    """
    return get_experiment().send_image(channel_name, x, y, name, description, timestamp)


def log_image(log_name, x, y=None, image_name=None, description=None, timestamp=None):
    """Log image data in Neptune.

    Alias for :meth:`~neptune.experiments.Experiment.log_image`
    """
    return get_experiment().send_image(log_name, x, y, image_name, description, timestamp)


def send_artifact(artifact, destination=None):
    """Save an artifact (file) in experiment storage.

    Alias for :meth:`~neptune.experiments.Experiment.log_artifact`
    """
    return get_experiment().log_artifact(artifact, destination)


def delete_artifacts(path):
    """Delete an artifact (file/directory) from experiment storage.

    Alias for :meth:`~neptune.experiments.Experiment.delete_artifacts`
    """
    return get_experiment().delete_artifacts(path)


def log_artifact(artifact, destination=None):
    """Save an artifact (file) in experiment storage.

    Alias for :meth:`~neptune.experiments.Experiment.log_artifact`
    """
    return get_experiment().log_artifact(artifact, destination)


def stop(traceback=None):
    """Marks experiment as finished (succeeded or failed).

    Alias for :meth:`~neptune.experiments.Experiment.stop`
    """
    get_experiment().stop(traceback)
