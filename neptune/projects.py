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

from platform import node as get_hostname

import click

import pandas as pd

from neptune.experiments import Experiment, push_new_experiment
from neptune.internal.abort import DefaultAbortImpl
from neptune.utils import as_list, map_keys, get_git_info, discover_git_repo_location


class Project(object):
    # pylint: disable=redefined-builtin

    """It contains all the information about a Neptune project

    You can extract the experiment view in a form of a dataframe or a list of experiments.
    Project lets you do filtering based on conditions not to fetch the entire, sometimes huge list of experiments.

    Args:
        client(`neptune.Client`): Client object
        internal_id:
        namespace(str): It can either be your organization or user name. You can list all the public projects for any
               organization or user you want as long as you know their namespace.
        name(str): short project name.

    Attributes:
        client(`neptune.Client`): Client object
        internal_id:
        namespace(str): It can either be your organization or user name. You can list all the public projects for any
               organization or user you want as long as you know their namespace.
        name(str): short project name.

    Examples:
        Instantiate a session.

        >>> from neptune.sessions import Session
        >>> session = Session()

        Fetch a project.

        >>> project = session.get_projects('neptune-ml')['neptune-ml/Salt-Detection']
        >>> project
        Project(neptune-ml/Salt-Detection)

    Todo:
        Drop the pylint line.
        Explain what internal_id is

    """

    def __init__(self, client, internal_id, namespace, name):
        self.client = client
        self.internal_id = internal_id
        self.namespace = namespace
        self.name = name

    def get_members(self):
        """Retrieve a list of project members.

        Returns:
            list: A list of usernames of project members.

        Examples:

            >>> project = session.get_projects('neptune-ml')['neptune-ml/Salt-Detection']
            >>> project.get_members()

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
            Experiment(self.client, entry.id, entry.internal_id, entry.project_full_id) for entry in leaderboard_entries
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
            id(list): An ID or list of experiment IDs (rowo.g. 'SAN-1' or ['SAN-1', 'SAN-2'])
            state(list): A state or list of experiment states.
                E.g. 'succeeded' or ['succeeded', 'preempted']
                Possible states: 'creating', 'waiting', 'initializing', 'running',
                    'cleaning', 'crashed', 'failed', 'aborted', 'preempted', 'succeeded'
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

        Todo:
            tags - is it ok now?
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
                          upload_stdout=True,
                          upload_stderr=True,
                          send_hardware_metrics=True,
                          run_monitoring_thread=True,
                          handle_uncaught_exceptions=True,
                          git_info=None,
                          hostname=None):
        """
        Raises:
            `ExperimentValidationError`: When provided arguments are invalid.
            `ExperimentLimitReached`: When experiment limit in the project has been reached.
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
            hostname=hostname
        )

        experiment.start(
            upload_source_files=upload_source_files,
            abort_callback=abort_callback,
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
        notebook = self.client.create_notebook(self)
        return notebook

    def get_notebook(self, notebook_id):
        return self.client.get_notebook(project=self, notebook_id=notebook_id)

    @property
    def full_id(self):
        """Creates a full project id by combining the namespace and project name.
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
