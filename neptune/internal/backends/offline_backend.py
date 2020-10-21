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
from io import StringIO

from neptune.backend import Backend

_logger = logging.getLogger(__name__)


class OfflineBackend(Backend):

    def __init__(self):
        _logger.warning('Neptune is running in offline mode. No data is being logged to Neptune.')
        _logger.warning('Disable offline mode to log your experiments.')

    @property
    def api_address(self):
        return 'OFFLINE'

    @property
    def display_address(self):
        return 'OFFLINE'

    @property
    def proxies(self):
        return None

    def get_project(self, project_qualified_name):
        return NoopObject()

    def get_projects(self, namespace):
        return []

    def get_project_members(self, project_identifier):
        return []

    def get_leaderboard_entries(self, project, entry_types, ids, states, owners, tags, min_running_time):
        return []

    def get_channel_points_csv(self, experiment, channel_internal_id):
        return StringIO()

    def get_metrics_csv(self, experiment):
        return StringIO()

    def create_experiment(self, project, name, description,
                          params, properties, tags, abortable,
                          monitored, git_info, hostname, entrypoint,
                          notebook_id, checkpoint_id):
        return NoopObject()

    def get_notebook(self, project, notebook_id):
        return NoopObject()

    def get_last_checkpoint(self, project, notebook_id):
        return NoopObject()

    def create_notebook(self, project):
        return NoopObject()

    def create_checkpoint(self, notebook_id, jupyter_path, _file):
        pass

    def get_experiment(self, experiment_id):
        return NoopObject()

    def update_experiment(self, experiment, properties):
        pass

    def update_tags(self, experiment, tags_to_add, tags_to_delete):
        pass

    def upload_experiment_source(self, experiment, data, progress_indicator):
        pass

    def extract_experiment_source(self, experiment, data):
        pass

    def create_channel(self, experiment, name, channel_type):
        return NoopObject()

    def reset_channel(self, channel_id):
        pass

    def create_system_channel(self, experiment, name, channel_type):
        return NoopObject()

    def get_system_channels(self, experiment):
        return []

    def send_channels_values(self, experiment, channels_with_values):
        pass

    def mark_succeeded(self, experiment):
        pass

    def mark_failed(self, experiment, traceback):
        pass

    def ping_experiment(self, experiment):
        pass

    def create_hardware_metric(self, experiment, metric):
        return NoopObject()

    def send_hardware_metric_reports(self, experiment, metrics, metric_reports):
        pass

    def upload_experiment_output(self, experiment, data, progress_indicator):
        pass

    def extract_experiment_output(self, experiment, data):
        pass

    def rm_data(self, experiment, path):
        pass

    def download_data(self, project, path, destination):
        pass


class NoopObject(object):

    def __getattr__(self, name):
        return self

    def __getitem__(self, key):
        return self

    def __call__(self, *args, **kwargs):
        return self

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
