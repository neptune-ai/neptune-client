#
# Copyright (c) 2022, Neptune Labs Sp. z o.o.
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

from abc import (
    ABC,
    abstractmethod,
)
from typing import Dict

from neptune.legacy.model import ChannelWithLastValue


class ApiClient(ABC):
    @property
    @abstractmethod
    def api_address(self):
        pass

    @property
    @abstractmethod
    def display_address(self):
        pass

    @property
    @abstractmethod
    def proxies(self):
        pass


class BackendApiClient(ApiClient, ABC):
    @abstractmethod
    def get_project(self, project_qualified_name):
        pass

    @abstractmethod
    def get_projects(self, namespace):
        pass

    @abstractmethod
    def create_leaderboard_backend(self, project) -> "LeaderboardApiClient":
        pass


class LeaderboardApiClient(ApiClient, ABC):
    @abstractmethod
    def get_project_members(self, project_identifier):
        pass

    @abstractmethod
    def get_leaderboard_entries(
        self,
        project,
        entry_types=None,
        ids=None,
        states=None,
        owners=None,
        tags=None,
        min_running_time=None,
    ):
        pass

    def websockets_factory(self, project_id, experiment_id):
        return None

    @abstractmethod
    def get_channel_points_csv(self, experiment, channel_internal_id, channel_name):
        pass

    @abstractmethod
    def get_metrics_csv(self, experiment):
        pass

    @abstractmethod
    def create_experiment(
        self,
        project,
        name,
        description,
        params,
        properties,
        tags,
        abortable,
        monitored,
        git_info,
        hostname,
        entrypoint,
        notebook_id,
        checkpoint_id,
    ):
        pass

    @abstractmethod
    def upload_source_code(self, experiment, source_target_pairs):
        pass

    @abstractmethod
    def get_notebook(self, project, notebook_id):
        pass

    @abstractmethod
    def get_last_checkpoint(self, project, notebook_id):
        pass

    @abstractmethod
    def create_notebook(self, project):
        pass

    @abstractmethod
    def create_checkpoint(self, notebook_id, jupyter_path, _file=None):
        pass

    @abstractmethod
    def get_experiment(self, experiment_id):
        pass

    @abstractmethod
    def set_property(self, experiment, key, value):
        pass

    @abstractmethod
    def remove_property(self, experiment, key):
        pass

    @abstractmethod
    def update_tags(self, experiment, tags_to_add, tags_to_delete):
        pass

    @abstractmethod
    def create_channel(self, experiment, name, channel_type) -> ChannelWithLastValue:
        pass

    @abstractmethod
    def get_channels(self, experiment) -> Dict[str, object]:
        pass

    @abstractmethod
    def reset_channel(self, experiment, channel_id, channel_name, channel_type):
        pass

    @abstractmethod
    def create_system_channel(self, experiment, name, channel_type) -> ChannelWithLastValue:
        pass

    @abstractmethod
    def get_system_channels(self, experiment) -> Dict[str, object]:
        pass

    @abstractmethod
    def send_channels_values(self, experiment, channels_with_values):
        pass

    @abstractmethod
    def mark_failed(self, experiment, traceback):
        pass

    @abstractmethod
    def ping_experiment(self, experiment):
        pass

    @abstractmethod
    def create_hardware_metric(self, experiment, metric):
        pass

    @abstractmethod
    def send_hardware_metric_reports(self, experiment, metrics, metric_reports):
        pass

    @abstractmethod
    def log_artifact(self, experiment, artifact, destination=None):
        pass

    @abstractmethod
    def delete_artifacts(self, experiment, path):
        pass

    @abstractmethod
    def download_data(self, experiment, path, destination):
        pass

    @abstractmethod
    def download_sources(self, experiment, path=None, destination_dir=None):
        pass

    @abstractmethod
    def download_artifacts(self, experiment, path=None, destination_dir=None):
        pass

    @abstractmethod
    def download_artifact(self, experiment, path=None, destination_dir=None):
        pass
