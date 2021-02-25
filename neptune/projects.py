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

import atexit
import logging
import os
import os.path
import threading
from platform import node as get_hostname

import click
import pandas as pd
import six

from neptune.envs import NOTEBOOK_ID_ENV_NAME, NOTEBOOK_PATH_ENV_NAME
from neptune.exceptions import NeptuneNoExperimentContextException
from neptune.experiments import Experiment
from neptune.internal.abort import DefaultAbortImpl
from neptune.internal.notebooks.notebooks import create_checkpoint
from neptune.internal.utils.source_code import get_source_code_to_upload
from neptune.utils import as_list, map_keys, get_git_info, discover_git_repo_location

_logger = logging.getLogger(__name__)


class Project(object):
    # pylint: disable=redefined-builtin

    """A class for storing information and managing Neptune project.

    Args:
        backend (:class:`~neptune.ApiClient`, required): A ApiClient object.
        internal_id (:obj:`str`, required): UUID of the project.
        namespace (:obj:`str`, required): It can either be your workspace or user name.
        name (:obj:`str`, required): project name.

    Note:
        ``namespace`` and ``name`` joined together with ``/`` form ``project_qualified_name``.
    """

    def __init__(self, backend, internal_id, namespace, name):
        self._backend = backend
        self.internal_id = internal_id
        self.namespace = namespace
        self.name = name

        self._experiments_stack = []
        self.__lock = threading.RLock()
        atexit.register(self._shutdown_hook)

    def get_members(self):
        """Retrieve a list of project members.

        Returns:
            :obj:`list` of :obj:`str` - A list of usernames of project members.

        Examples:

            .. code:: python3

                project = session.get_projects('neptune-ai')['neptune-ai/Salt-Detection']
                project.get_members()

        """
        project_members = self._backend.get_project_members(self.internal_id)
        return [member.registeredMemberInfo.username for member in project_members if member.registeredMemberInfo]

    def get_experiments(self, id=None, state=None, owner=None, tag=None, min_running_time=None):
        """Retrieve list of experiments matching the specified criteria.

        All parameters are optional, each of them specifies a single criterion.
        Only experiments matching all of the criteria will be returned.

        Args:
            id (:obj:`str` or :obj:`list` of :obj:`str`, optional, default is ``None``):
                | An experiment id like ``'SAN-1'`` or list of ids like ``['SAN-1', 'SAN-2']``.
                | Matching any element of the list is sufficient to pass criterion.
            state (:obj:`str` or :obj:`list` of :obj:`str`, optional, default is ``None``):
                | An experiment state like ``'succeeded'`` or list of states like ``['succeeded', 'running']``.
                | Possible values: ``'running'``, ``'succeeded'``, ``'failed'``, ``'aborted'``.
                | Matching any element of the list is sufficient to pass criterion.
            owner (:obj:`str` or :obj:`list` of :obj:`str`, optional, default is ``None``):
                | *Username* of the experiment owner (User who created experiment is an owner) like ``'josh'``
                  or list of owners like ``['frederic', 'josh']``.
                | Matching any element of the list is sufficient to pass criterion.
            tag (:obj:`str` or :obj:`list` of :obj:`str`, optional, default is ``None``):
                 | An experiment tag like ``'lightGBM'`` or list of tags like ``['pytorch', 'cycleLR']``.
                 | Only experiments that have all specified tags will match this criterion.
            min_running_time (:obj:`int`, optional, default is ``None``):
                Minimum running time of an experiment in seconds, like ``2000``.

        Returns:
            :obj:`list` of :class:`~neptune.experiments.Experiment` objects.

        Examples:

            .. code:: python3

                # Fetch a project
                project = session.get_projects('neptune-ai')['neptune-ai/Salt-Detection']

                # Get list of experiments
                project.get_experiments(state=['aborted'], owner=['neyo'], min_running_time=100000)

                # Example output:
                # [Experiment(SAL-1609),
                #  Experiment(SAL-1765),
                #  Experiment(SAL-1941),
                #  Experiment(SAL-1960),
                #  Experiment(SAL-2025)]
        """
        leaderboard_entries = self._fetch_leaderboard(id, state, owner, tag, min_running_time)
        return [
            Experiment(self._backend, self, entry.id, entry.internal_id)
            for entry in leaderboard_entries
        ]

    def get_leaderboard(self, id=None, state=None, owner=None, tag=None, min_running_time=None):
        """Fetch Neptune experiments view as pandas ``DataFrame``.

        **returned DataFrame**

        | In the returned ``DataFrame`` each *row* is an experiment and *columns* represent all system properties,
          numeric and text logs, parameters and properties in these experiments.
        | Note that, returned ``DataFrame`` does not contain all columns across the entire project.
        | Some columns may be empty, since experiments may define various logs, properties, etc.
        | For each log at most one (the last one) value is returned per experiment.
        | Text values are trimmed to 255 characters.

        **about parameters**

        All parameters are optional, each of them specifies a single criterion.
        Only experiments matching all of the criteria will be returned.

        Args:
            id (:obj:`str` or :obj:`list` of :obj:`str`, optional, default is ``None``):
                | An experiment id like ``'SAN-1'`` or list of ids like ``['SAN-1', 'SAN-2']``.
                | Matching any element of the list is sufficient to pass criterion.
            state (:obj:`str` or :obj:`list` of :obj:`str`, optional, default is ``None``):
                | An experiment state like ``'succeeded'`` or list of states like ``['succeeded', 'running']``.
                | Possible values: ``'running'``, ``'succeeded'``, ``'failed'``, ``'aborted'``.
                | Matching any element of the list is sufficient to pass criterion.
            owner (:obj:`str` or :obj:`list` of :obj:`str`, optional, default is ``None``):
                | *Username* of the experiment owner (User who created experiment is an owner) like ``'josh'``
                  or list of owners like ``['frederic', 'josh']``.
                | Matching any element of the list is sufficient to pass criterion.
            tag (:obj:`str` or :obj:`list` of :obj:`str`, optional, default is ``None``):
                | An experiment tag like ``'lightGBM'`` or list of tags like ``['pytorch', 'cycleLR']``.
                | Only experiments that have all specified tags will match this criterion.
            min_running_time (:obj:`int`, optional, default is ``None``):
                Minimum running time of an experiment in seconds, like ``2000``.

        Returns:
            :obj:`pandas.DataFrame` - Fetched Neptune experiments view.

        Examples:

            .. code:: python3

                # Fetch a project.
                project = session.get_projects('neptune-ai')['neptune-ai/Salt-Detection']

                # Get DataFrame that resembles experiment view.
                project.get_leaderboard(state=['aborted'], owner=['neyo'], min_running_time=100000)
        """

        leaderboard_entries = self._fetch_leaderboard(id, state, owner, tag, min_running_time)

        def make_row(entry):
            channels = dict(
                ('channel_{}'.format(ch.name), ch.trimmed_y) for ch in entry.channels
            )

            parameters = map_keys('parameter_{}'.format, entry.parameters)
            properties = map_keys('property_{}'.format, entry.properties)

            r = {}
            r.update(entry.system_properties)
            r.update(channels)
            r.update(parameters)
            r.update(properties)
            return r

        rows = ((n, make_row(e)) for (n, e) in enumerate(leaderboard_entries))

        df = pd.DataFrame.from_dict(data=dict(rows), orient='index')
        df = df.reindex(self._sort_leaderboard_columns(df.columns), axis='columns')
        return df

    def create_experiment(self,
                          name=None,
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
                          notebook_id=None,
                          notebook_path=None):
        """Create and start Neptune experiment.

        Create experiment, set its status to `running` and append it to the top of the experiments view.
        All parameters are optional, hence minimal invocation: ``neptune.create_experiment()``.

        Args:
            name (:obj:`str`, optional, default is ``'Untitled'``):
                Editable name of the experiment.
                Name is displayed in the experiment's `Details` (`Metadata` section)
                and in `experiments view` as a column.

            description (:obj:`str`, optional, default is ``''``):
                Editable description of the experiment.
                Description is displayed in the experiment's `Details` (`Metadata` section)
                and can be displayed in the `experiments view` as a column.

            params (:obj:`dict`, optional, default is ``{}``):
                Parameters of the experiment.
                After experiment creation ``params`` are read-only
                (see: :meth:`~neptune.experiments.Experiment.get_parameters`).
                Parameters are displayed in the experiment's `Details` (`Parameters` section)
                and each key-value pair can be viewed in `experiments view` as a column.

            properties (:obj:`dict`, optional, default is ``{}``):
                Properties of the experiment.
                They are editable after experiment is created.
                Properties are displayed in the experiment's `Details` (`Properties` section)
                and each key-value pair can be viewed in `experiments view` as a column.

            tags (:obj:`list`, optional, default is ``[]``):
                Must be list of :obj:`str`. Tags of the experiment.
                They are editable after experiment is created
                (see: :meth:`~neptune.experiments.Experiment.append_tag`
                and :meth:`~neptune.experiments.Experiment.remove_tag`).
                Tags are displayed in the experiment's `Details` (`Metadata` section)
                and can be viewed in `experiments view` as a column.

            upload_source_files (:obj:`list` or :obj:`str`, optional, default is ``None``):
                List of source files to be uploaded. Must be list of :obj:`str` or single :obj:`str`.
                Uploaded sources are displayed in the experiment's `Source code` tab.

                | If ``None`` is passed, Python file from which experiment was created will be uploaded.
                | Pass empty list (``[]``) to upload no files.
                | Unix style pathname pattern expansion is supported. For example, you can pass ``'*.py'`` to upload
                  all python source files from the current directory.
                  For Python 3.5 or later, paths of uploaded files on server are resolved as relative to the
                | calculated common root of all uploaded source  files. For older Python versions, paths on server are
                | resolved always as relative to the current directory.
                  For recursion lookup use ``'**/*.py'`` (for Python 3.5 and later).
                  For more information see `glob library <https://docs.python.org/3/library/glob.html>`_.

            abort_callback (:obj:`callable`, optional, default is ``None``):
                Callback that defines how `abort experiment` action in the Web application should work.
                Actual behavior depends on your setup:

                    * (default) If ``abort_callback=None`` and `psutil <https://psutil.readthedocs.io/en/latest/>`_
                      is installed, then current process and it's children are aborted by sending `SIGTERM`.
                      If, after grace period, processes are not terminated, `SIGKILL` is sent.
                    * If ``abort_callback=None`` and `psutil <https://psutil.readthedocs.io/en/latest/>`_
                      is **not** installed, then `abort experiment` action just marks experiment as *aborted*
                      in the Web application. No action is performed on the current process.
                    * If ``abort_callback=callable``, then ``callable`` is executed when `abort experiment` action
                      in the Web application is triggered.

            logger (:obj:`logging.Logger` or `None`, optional, default is ``None``):
                If Python's `Logger <https://docs.python.org/3/library/logging.html#logging.Logger>`_
                is passed, new experiment's `text log`
                (see: :meth:`~neptune.experiments.Experiment.log_text`) with name `"logger"` is created.
                Each time `Python logger` logs new data, it is automatically sent to the `"logger"` in experiment.
                As a results all data from `Python logger` are in the `Logs` tab in the experiment.

            upload_stdout (:obj:`Boolean`, optional, default is ``True``):
                Whether to send stdout to experiment's *Monitoring*.

            upload_stderr (:obj:`Boolean`, optional, default is ``True``):
                Whether to send stderr to experiment's *Monitoring*.

            send_hardware_metrics (:obj:`Boolean`, optional, default is ``True``):
                Whether to send hardware monitoring logs (CPU, GPU, Memory utilization) to experiment's *Monitoring*.

            run_monitoring_thread (:obj:`Boolean`, optional, default is ``True``):
                Whether to run thread that pings Neptune server in order to determine if experiment is responsive.

            handle_uncaught_exceptions (:obj:`Boolean`, optional, default is ``True``):
                Two options ``True`` and ``False`` are possible:

                    * If set to ``True`` and uncaught exception occurs, then Neptune automatically place
                      `Traceback` in the experiment's `Details` and change experiment status to `Failed`.
                    * If set to ``False`` and uncaught exception occurs, then no action is performed
                      in the Web application. As a consequence, experiment's status is `running` or `not responding`.

            git_info (:class:`~neptune.git_info.GitInfo`, optional, default is ``None``):

                | Instance of the class :class:`~neptune.git_info.GitInfo` that provides information about
                  the git repository from which experiment was started.
                | If ``None`` is passed,
                  system attempts to automatically extract information about git repository in the following way:

                      * System looks for `.git` file in the current directory and, if not found,
                        goes up recursively until `.git` file will be found
                        (see: :meth:`~neptune.utils.get_git_info`).
                      * If there is no git repository,
                        then no information about git is displayed in experiment details in Neptune web application.

            hostname (:obj:`str`, optional, default is ``None``):
                If ``None``, neptune automatically get `hostname` information.
                User can also set `hostname` directly by passing :obj:`str`.

        Returns:
            :class:`~neptune.experiments.Experiment` object that is used to manage experiment and log data to it.

        Raises:
            `ExperimentValidationError`: When provided arguments are invalid.
            `ExperimentLimitReached`: When experiment limit in the project has been reached.

        Examples:

            .. code:: python3

                # minimal invoke
                neptune.create_experiment()

                # explicitly return experiment object
                experiment = neptune.create_experiment()

                # create experiment with name and two parameters
                neptune.create_experiment(name='first-pytorch-ever',
                                          params={'lr': 0.0005,
                                                  'dropout': 0.2})

                # create experiment with name and description, and no sources files uploaded
                neptune.create_experiment(name='neural-net-mnist',
                                          description='neural net trained on MNIST',
                                          upload_source_files=[])

                # Send all py files in cwd (excluding hidden files with names beginning with a dot)
                neptune.create_experiment(upload_source_files='*.py')

                # Send all py files from all subdirectories (excluding hidden files with names beginning with a dot)
                # Supported on Python 3.5 and later.
                neptune.create_experiment(upload_source_files='**/*.py')

                # Send all files and directories in cwd (excluding hidden files with names beginning with a dot)
                neptune.create_experiment(upload_source_files='*')

                # Send all files and directories in cwd including hidden files
                neptune.create_experiment(upload_source_files=['*', '.*'])

                # Send files with names being a single character followed by '.py' extension.
                neptune.create_experiment(upload_source_files='?.py')

                # larger example
                neptune.create_experiment(name='first-pytorch-ever',
                                          params={'lr': 0.0005,
                                                  'dropout': 0.2},
                                          properties={'key1': 'value1',
                                                      'key2': 17,
                                                      'key3': 'other-value'},
                                          description='write longer description here',
                                          tags=['list-of', 'tags', 'goes-here', 'as-list-of-strings'],
                                          upload_source_files=['training_with_pytorch.py', 'net.py'])
        """

        if name is None:
            name = "Untitled"

        if description is None:
            description = ""

        if params is None:
            params = {}

        if properties is None:
            properties = {}

        if tags is None:
            tags = []

        if git_info is None:
            git_info = get_git_info(discover_git_repo_location())

        if hostname is None:
            hostname = get_hostname()

        if notebook_id is None and os.getenv(NOTEBOOK_ID_ENV_NAME, None) is not None:
            notebook_id = os.environ[NOTEBOOK_ID_ENV_NAME]

        if isinstance(upload_source_files, six.string_types):
            upload_source_files = [upload_source_files]

        entrypoint, source_target_pairs = get_source_code_to_upload(upload_source_files=upload_source_files)

        if notebook_path is None and os.getenv(NOTEBOOK_PATH_ENV_NAME, None) is not None:
            notebook_path = os.environ[NOTEBOOK_PATH_ENV_NAME]

        abortable = abort_callback is not None or DefaultAbortImpl.requirements_installed()

        checkpoint_id = None
        if notebook_id is not None and notebook_path is not None:
            checkpoint = create_checkpoint(backend=self._backend,
                                           notebook_id=notebook_id,
                                           notebook_path=notebook_path)
            if checkpoint is not None:
                checkpoint_id = checkpoint.id

        experiment = self._backend.create_experiment(
            project=self,
            name=name,
            description=description,
            params=params,
            properties=properties,
            tags=tags,
            abortable=abortable,
            monitored=run_monitoring_thread,
            git_info=git_info,
            hostname=hostname,
            entrypoint=entrypoint,
            notebook_id=notebook_id,
            checkpoint_id=checkpoint_id
        )

        self._backend.upload_source_code(experiment, source_target_pairs)

        # pylint: disable=protected-access
        experiment._start(
            abort_callback=abort_callback,
            logger=logger,
            upload_stdout=upload_stdout,
            upload_stderr=upload_stderr,
            send_hardware_metrics=send_hardware_metrics,
            run_monitoring_thread=run_monitoring_thread,
            handle_uncaught_exceptions=handle_uncaught_exceptions
        )

        self._push_new_experiment(experiment)

        click.echo(self._get_experiment_link(experiment))

        return experiment

    def _get_experiment_link(self, experiment):
        return "{base_url}/{namespace}/{project}/e/{exp_id}".format(
            base_url=self._backend.display_address,
            namespace=self.namespace,
            project=self.name,
            exp_id=experiment.id
        )

    def create_notebook(self):
        """Create a new notebook object and return corresponding :class:`~neptune.notebook.Notebook` instance.

        Returns:
            :class:`~neptune.notebook.Notebook` object.

        Examples:

            .. code:: python3

                # Instantiate a session and fetch a project
                project = neptune.init()

                # Create a notebook in Neptune
                notebook = project.create_notebook()
        """
        return self._backend.create_notebook(self)

    def get_notebook(self, notebook_id):
        """Get a :class:`~neptune.notebook.Notebook` object with given ``notebook_id``.

        Returns:
            :class:`~neptune.notebook.Notebook` object.

        Examples:

            .. code:: python3

                # Instantiate a session and fetch a project
                project = neptune.init()

                # Get a notebook object
                notebook = project.get_notebook('d1c1b494-0620-4e54-93d5-29f4e848a51a')
        """
        return self._backend.get_notebook(project=self, notebook_id=notebook_id)

    @property
    def full_id(self):
        """Project qualified name as :obj:`str`, for example `john/sandbox`.
        """
        return '{}/{}'.format(self.namespace, self.name)

    def __str__(self):
        return 'Project({})'.format(self.full_id)

    def __repr__(self):
        return str(self)

    def __eq__(self, o):
        return self.__dict__ == o.__dict__

    def __ne__(self, o):
        return not self.__eq__(o)

    def _fetch_leaderboard(self, id, state, owner, tag, min_running_time):
        return self._backend.get_leaderboard_entries(
            project=self, ids=as_list(id), states=as_list(state),
            owners=as_list(owner), tags=as_list(tag),
            min_running_time=min_running_time)

    @staticmethod
    def _sort_leaderboard_columns(column_names):
        user_defined_weights = {
            'channel': 1,
            'parameter': 2,
            'property': 3
        }

        system_properties_weights = {
            'id': 0,
            'name': 1,
            'created': 2,
            'finished': 3,
            'owner': 4,
            'worker_type': 5,
            'environment': 6,
        }

        def key(c):
            """A sorting key for a column name.

            Sorts by the system properties first, then channels, parameters, user-defined properties.

            Within each group columns are sorted alphabetically, except for system properties,
            where order is custom.
            """
            parts = c.split('_', 1)
            if parts[0] in user_defined_weights.keys():
                name = parts[1]
                weight = user_defined_weights.get(parts[0], 99)
                system_property_weight = None
            else:
                name = c
                weight = 0
                system_property_weight = system_properties_weights.get(name, 99)

            return weight, system_property_weight, name

        return sorted(column_names, key=key)

    def _get_current_experiment(self):
        with self.__lock:
            if self._experiments_stack:
                return self._experiments_stack[-1]
            else:
                raise NeptuneNoExperimentContextException()

    def _push_new_experiment(self, new_experiment):
        with self.__lock:
            self._experiments_stack.append(new_experiment)
            return new_experiment

    def _remove_stopped_experiment(self, experiment):
        with self.__lock:
            if self._experiments_stack:
                self._experiments_stack = [exp for exp in self._experiments_stack if exp != experiment]

    def _shutdown_hook(self):
        if self._experiments_stack:
            # stopping experiment removes it from list, co we copy it
            copied_experiment_list = [exp for exp in self._experiments_stack]
            for exp in copied_experiment_list:
                exp.stop()
