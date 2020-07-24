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

from abc import ABCMeta, abstractmethod, abstractproperty

import six


@six.add_metaclass(ABCMeta)
class Backend(object):

    @abstractproperty
    def api_address(self):
        pass

    @abstractproperty
    def display_address(self):
        pass

    @abstractproperty
    def proxies(self):
        pass

    @abstractmethod
    def get_project(self, project_qualified_name):
        pass

    @abstractmethod
    def get_projects(self, namespace):
        pass

    @abstractmethod
    def get_project_members(self, project_identifier):
        pass

    @abstractmethod
    def get_leaderboard_entries(self, project, entry_types, ids, states, owners, tags, min_running_time):
        pass

    @abstractmethod
    def get_channel_points_csv(self, experiment, channel_internal_id):
        pass

    @abstractmethod
    def get_metrics_csv(self, experiment):
        pass

    @abstractmethod
    def create_experiment(self, project, name, description,
                          params, properties, tags, abortable,
                          monitored, git_info, hostname, entrypoint,
                          notebook_id, checkpoint_id):
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
    def create_checkpoint(self, notebook_id, jupyter_path, _file):
        pass

    @abstractmethod
    def get_experiment(self, experiment_id):
        pass

    @abstractmethod
    def update_experiment(self, experiment, properties):
        pass

    @abstractmethod
    def update_tags(self, experiment, tags_to_add, tags_to_delete):
        pass

    @abstractmethod
    def upload_experiment_source(self, experiment, data, progress_indicator):
        pass

    @abstractmethod
    def extract_experiment_source(self, experiment, data):
        pass

    @abstractmethod
    def create_channel(self, experiment, name, channel_type):
        pass

    @abstractmethod
    def reset_channel(self, channel_id):
        pass

    @abstractmethod
    def create_system_channel(self, experiment, name, channel_type):
        pass

    @abstractmethod
    def get_system_channels(self, experiment):
        pass

    @abstractmethod
    def send_channels_values(self, experiment, channels_with_values):
        pass

    @abstractmethod
    def mark_succeeded(self, experiment):
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
    def upload_experiment_output(self, experiment, data, progress_indicator):
        pass

    @abstractmethod
    def extract_experiment_output(self, experiment, data):
        pass

    @abstractmethod
    def download_data(self, project, path, destination):
        pass
