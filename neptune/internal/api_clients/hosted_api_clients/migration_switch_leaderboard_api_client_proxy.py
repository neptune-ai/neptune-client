#
# Copyright (c) 2021, Neptune Labs Sp. z o.o.
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
import threading
from functools import wraps
from typing import Dict, TYPE_CHECKING

from neptune.model import ChannelWithLastValue

from neptune.backend import LeaderboardApiClient

from neptune.exceptions import ProjectMigratedToNewStructure
from neptune.internal.api_clients.hosted_api_clients.hosted_leaderboard_api_client import \
    HostedNeptuneLeaderboardApiClient

if TYPE_CHECKING:
    from neptune.internal.api_clients import HostedNeptuneBackendApiClient

_logger = logging.getLogger(__name__)


# pylint: disable=protected-access
def with_migration_handling(func):
    @wraps(func)
    def wrapper(api_proxy: 'MigrationSwitchLeaderboardApiClientProxy', *args, **kwargs):
        try:
            return func(api_proxy, *args, **kwargs)
        except ProjectMigratedToNewStructure:
            if not api_proxy._switched:
                api_proxy._lock.acquire()
                if not api_proxy._switched:
                    api_proxy._client = api_proxy._backend_client.get_new_leaderboard_client()
                    api_proxy._switched = True
                api_proxy._lock.release()
            return func(api_proxy, *args, **kwargs)

    return wrapper


class MigrationSwitchLeaderboardApiClientProxy(LeaderboardApiClient):

    def __init__(self, api_client: HostedNeptuneLeaderboardApiClient, backend_client: 'HostedNeptuneBackendApiClient'):
        self._client = api_client
        self._backend_client = backend_client
        self._lock = threading.RLock()
        self._switched = False

    @property
    def http_client(self):
        return self._client.http_client

    @property
    def backend_client(self):
        return self._client.backend_client

    @property
    def authenticator(self):
        return self._client.authenticator

    @property
    def credentials(self):
        return self._client.credentials

    @property
    def backend_swagger_client(self):
        return self._client.backend_swagger_client

    @property
    def client_lib_version(self):
        return self._client.client_lib_version

    @property
    def api_address(self):
        return self._client.api_address

    @property
    def display_address(self):
        return self._client.display_address

    @property
    def proxies(self):
        return self._client.proxies

    @with_migration_handling
    def get_project_members(self, project_identifier):
        return self._client.get_project_members(project_identifier)

    @with_migration_handling
    def get_leaderboard_entries(self, project,
                                entry_types=None, ids=None,
                                states=None, owners=None, tags=None,
                                min_running_time=None):
        return self._client.get_leaderboard_entries(project, entry_types, ids, states, owners, tags, min_running_time)

    @with_migration_handling
    def websockets_factory(self, project_uuid, experiment_id):
        return self._client.websockets_factory(project_uuid, experiment_id)

    @with_migration_handling
    def get_channel_points_csv(self, experiment, channel_internal_id, channel_name):
        return self._client.get_channel_points_csv(experiment, channel_internal_id, channel_name)

    @with_migration_handling
    def get_metrics_csv(self, experiment):
        return self._client.get_metrics_csv(experiment)

    @with_migration_handling
    def create_experiment(self, project, name, description,
                          params, properties, tags, abortable,
                          monitored, git_info, hostname, entrypoint,
                          notebook_id, checkpoint_id):
        return self._client.create_experiment(
            project, name, description,
            params, properties, tags, abortable,
            monitored, git_info, hostname, entrypoint,
            notebook_id, checkpoint_id)

    @with_migration_handling
    def upload_source_code(self, experiment, source_target_pairs):
        return self._client.upload_source_code(experiment, source_target_pairs)

    @with_migration_handling
    def get_notebook(self, project, notebook_id):
        return self._client.get_notebook(project, notebook_id)

    @with_migration_handling
    def get_last_checkpoint(self, project, notebook_id):
        return self._client.get_last_checkpoint(project, notebook_id)

    @with_migration_handling
    def create_notebook(self, project):
        return self._client.create_notebook(project)

    @with_migration_handling
    def create_checkpoint(self, notebook_id, jupyter_path, _file=None):
        return self._client.create_checkpoint(notebook_id, jupyter_path, _file)

    @with_migration_handling
    def get_experiment(self, experiment_id):
        return self._client.get_experiment(experiment_id)

    @with_migration_handling
    def update_experiment(self, experiment, properties):
        return self._client.update_experiment(experiment, properties)

    @with_migration_handling
    def set_property(self, experiment, key, value):
        return self._client.set_property(experiment, key, value)

    @with_migration_handling
    def remove_property(self, experiment, key):
        return self._client.remove_property(experiment, key)

    @with_migration_handling
    def update_tags(self, experiment, tags_to_add, tags_to_delete):
        return self._client.update_tags(experiment, tags_to_add, tags_to_delete)

    @with_migration_handling
    def upload_experiment_source(self, experiment, data, progress_indicator):
        return self._client.upload_experiment_source(experiment, data, progress_indicator)

    @with_migration_handling
    def extract_experiment_source(self, experiment, data):
        return self._client.extract_experiment_source(experiment, data)

    @with_migration_handling
    def create_channel(self, experiment, name, channel_type) -> ChannelWithLastValue:
        return self._client.create_channel(experiment, name, channel_type)

    @with_migration_handling
    def get_channels(self, experiment) -> Dict[str, object]:
        return self._client.get_channels(experiment)

    @with_migration_handling
    def reset_channel(self, experiment, channel_id, channel_name, channel_type):
        return self._client.reset_channel(experiment, channel_id, channel_name, channel_type)

    @with_migration_handling
    def create_system_channel(self, experiment, name, channel_type) -> ChannelWithLastValue:
        return self._client.create_system_channel(experiment, name, channel_type)

    @with_migration_handling
    def get_system_channels(self, experiment) -> Dict[str, object]:
        return self._client.get_system_channels(experiment)

    @with_migration_handling
    def send_channels_values(self, experiment, channels_with_values):
        return self._client.send_channels_values(experiment, channels_with_values)

    @with_migration_handling
    def mark_succeeded(self, experiment):
        return self._client.mark_succeeded(experiment)

    @with_migration_handling
    def mark_failed(self, experiment, traceback):
        return self._client.mark_failed(experiment, traceback)

    @with_migration_handling
    def ping_experiment(self, experiment):
        return self._client.ping_experiment(experiment)

    @with_migration_handling
    def create_hardware_metric(self, experiment, metric):
        return self._client.create_hardware_metric(experiment, metric)

    @with_migration_handling
    def send_hardware_metric_reports(self, experiment, metrics, metric_reports):
        return self._client.send_hardware_metric_reports(experiment, metrics, metric_reports)

    @with_migration_handling
    def log_artifact(self, experiment, artifact, destination=None):
        return self._client.log_artifact(experiment, artifact, destination)

    @with_migration_handling
    def delete_artifacts(self, experiment, path):
        return self._client.delete_artifacts(experiment, path)

    @with_migration_handling
    def download_data(self, experiment, path, destination):
        return self._client.download_data(experiment, path, destination)

    @with_migration_handling
    def download_sources(self, experiment, path=None, destination_dir=None):
        return self._client.download_sources(experiment, path, destination_dir)

    @with_migration_handling
    def download_artifacts(self, experiment, path=None, destination_dir=None):
        return self._client.download_artifacts(experiment, path, destination_dir)

    @with_migration_handling
    def download_artifact(self, experiment, path=None, destination_dir=None):
        return self._client.download_artifact(experiment, path, destination_dir)
