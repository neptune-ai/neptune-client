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
from platform import node as get_hostname

import click
import pandas as pd

from neptune.envs import NOTEBOOK_ID_ENV_NAME
from neptune.experiments import Experiment, push_new_experiment
from neptune.internal.abort import DefaultAbortImpl
from neptune.utils import as_list, map_keys, get_git_info, discover_git_repo_location


class Project(object):
    # pylint: disable=redefined-builtin

    """A class for storing information and managing Neptune project.

    Args:
        client (:class:`~neptune.client.Client`, required): Client object.
        internal_id (:obj:`str`, required): UUID of the project.
        namespace (:obj:`str`, required): It can either be your organization or user name.
        name (:obj:`str`, required): project name.

    Attributes:
        client (:class:`~neptune.client.Client`, required): Client object.
        internal_id (:obj:`str`, required): UUID of the project.
        namespace (:obj:`str`, required): It can either be your organization or user name.
        name (:obj:`str`, required): project name.

    Note:
        ``namespace`` and ``name`` joined together form ``project_qualified_name``.
    """

    def __init__(self, client, internal_id, namespace, name):
        self.client = client
        self.internal_id = internal_id
        self.namespace = namespace
        self.name = name

    def get_members(self):
        """Retrieve a list of project members.

        Returns:
            :obj:`list` of :obj:`str` - A list of usernames of project members.

        Examples:

            .. code:: python3

                project = session.get_projects('neptune-ml')['neptune-ml/Salt-Detection']
                project.get_members()

        """
        project_members = self.client.get_project_members(self.internal_id)
        return [member.registeredMemberInfo.username for member in project_members if member.registeredMemberInfo]

    def get_experiments(self, id=None, state=None, owner=None, tag=None, min_running_time=None):
        """Retrieve a list of experiments matching the specified criteria.

        All of the parameters of this method are optional, each of them specifies a single criterion.

        Only experiments matching all of the criteria will be returned.

        If a specific criterion accepts a list (like `state`), experiments matching any element of the list
        match this criterion.

        Args:
            id(list): An ID or list of experiment IDs (rowo.g. 'SAN-1' or ['SAN-1', 'SAN-2'])
            state(list): A state or list of experiment states.
                E.g. 'succeeded' or ['succeeded', 'preempted'].
                Possible states: 'creating', 'waiting', 'initializing', 'running', 'cleaning',
                'crashed', 'failed', 'aborted', 'preempted', 'succeeded'
            owner(list): The owner or list of owners of the experiments. This parameter expects usernames.
            tag(list): A tag or a list of experiment tags. E.g. 'solution-1' or ['solution-1', 'solution-2'].
            min_running_time(int): Minimum running time of an experiment in seconds.

        Returns:
            list: List of `Experiment` objects

        Examples:
            Instantiate a session.

            >>> from neptune.sessions import Session
            >>> session = Session()

            Fetch a project.

            >>> project = session.get_projects('neptune-ml')['neptune-ml/Salt-Detection']

            Finally, get a list of experiments that satisfies your criteria:

            >>> project.get_experiments(state=['aborted'], owner=['neyo'], min_running_time=100000)
            [Experiment(SAL-1609),
             Experiment(SAL-1765),
             Experiment(SAL-1941),
             Experiment(SAL-1960),
             Experiment(SAL-2025)]

        """
        leaderboard_entries = self._fetch_leaderboard(id, state, owner, tag, min_running_time)
        return [
            Experiment(self.client, self, entry.id, entry.internal_id)
            for entry in leaderboard_entries
        ]

    def get_leaderboard(self, id=None, state=None, owner=None, tag=None, min_running_time=None):
        """Fetches Neptune experiment view to pandas DataFrame

        Retrieve experiments matching the specified criteria and present them in a form of a DataFrame
        resembling Neptune's leaderboard.

        The returned DataFrame contains columns for all system properties,
        numeric and text channels, user-defined properties and parameters defined
        in the selected experiments (not across the entire project).

        Every row in this DataFrame represents a single experiment. As such, some columns may be empty,
        since experiments define various channels, properties, etc.

        For each channel at most one (the last one) value is returned per experiment.
        Text values are trimmed to 255 characters.

        All of the parameters of this method are optional, each of them specifies a single criterion.

        Only experiments matching all of the criteria will be returned.

        If a specific criterion accepts a list (like `state`), experiments matching any element of the list
        match this criterion.

        Args:
            id(list): An ID or list of experiment IDs ('SAN-1' or ['SAN-1', 'SAN-2'])
            state(list): A state or list of experiment states.
                E.g. 'succeeded' or ['succeeded', 'preempted']
                Possible states: 'running', 'failed', 'aborted', 'succeeded'.
            owner(list): The owner or list of owners of the experiments. This parameter expects usernames.
            tag(list): A tag or a list of experiment tags. E.g. 'solution-1' or ['solution-1', 'solution-2'].
            min_running_time(int): Minimum running time of an experiment in seconds.

        Returns:
            `pandas.DataFrame`: Neptune experiment view in the form of a dataframe.

        Examples:
            Instantiate a session.

            >>> from neptune.sessions import Session
            >>> session = Session()

            Fetch a project.

            >>> project = session.get_projects('neptune-ml')['neptune-ml/Salt-Detection']

            Finally, get a dataframe that resembles experiment view. It is constructed from all the
            experiments that satisfy your criteria:

            >>> project.get_leaderboard(state=['aborted'], owner=['neyo'], min_running_time=100000)
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
                          notebook_id=None):
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

            upload_source_files (:obj:`list`, optional, default is ``['main.py']``):
                | Where `'main.py'` is Python file from which experiment was created
                  (name `'main.py'` is just an example here).
                  Must be list of :obj:`str`.
                  Uploaded sources are displayed in the experiment's `Source code` tab.
                | Pass empty list (``[]``) to upload no files.

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

            logger (:obj:`logging.handlers` or `None`, optional, default is ``None``):
                If `handler <https://docs.python.org/3.6/library/logging.handlers.html>`_
                to `Python logger` is passed, new experiment's `text log`
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

        abortable = abort_callback is not None or DefaultAbortImpl.requirements_installed()

        experiment = self.client.create_experiment(
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
            notebook_id=notebook_id
        )

        experiment.start(
            upload_source_files=upload_source_files,
            abort_callback=abort_callback,
            logger=logger,
            upload_stdout=upload_stdout,
            upload_stderr=upload_stderr,
            send_hardware_metrics=send_hardware_metrics,
            run_monitoring_thread=run_monitoring_thread,
            handle_uncaught_exceptions=handle_uncaught_exceptions
        )

        push_new_experiment(experiment)

        click.echo(str(experiment.id))
        click.echo(self._get_experiment_link(experiment))

        return experiment

    def _get_experiment_link(self, experiment):
        return "{base_url}/{namespace}/{project}/e/{exp_id}".format(
            base_url=self.client.api_address,
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
        return self.client.create_notebook(self)

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
        return self.client.get_notebook(project=self, notebook_id=notebook_id)

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
        return self.client.get_leaderboard_entries(
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
