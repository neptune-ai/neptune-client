#
# Copyright (c) 2021, self.experiment Labs Sp. z o.o.
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

"""Remember to set environment values:
* self.experiment_API_TOKEN
* self.experiment_PROJECT
"""
import os

from alpha_integration_dev.old_client_noglobal import OldClientNonglobalFeatures
from neptune import Session, envs
from neptune.internal.backends.hosted_neptune_backend import HostedNeptuneBackend


class OldClientNonglobalDeprecatedFeatures(OldClientNonglobalFeatures):
    def __init__(self):
        backend = HostedNeptuneBackend()
        self.session = Session(backend=backend)
        self.project = self.session.get_project(os.getenv(envs.PROJECT_ENV_NAME))
        self.experiment = self.project.create_experiment(
            name='const project name',
            params=self.params,
            tags=['initial tag 1', 'initial tag 2'],
        )


if __name__ == '__main__':
    OldClientNonglobalDeprecatedFeatures().run()
