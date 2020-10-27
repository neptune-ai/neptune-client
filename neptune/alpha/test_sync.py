#
# Copyright (c) 2020, Neptune Labs Sp. z o.o.
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

from pathlib import Path
from tempfile import TemporaryDirectory

from neptune.alpha.internal.backends.hosted_neptune_backend import HostedNeptuneBackend
from neptune.alpha.internal.credentials import Credentials
from neptune.alpha.internal.utils.experiment_offset import ExperimentOffset
from neptune.alpha.internal.utils import experiment_offset
from neptune.alpha.sync import partition_experiments, list_experiments

# hubert3@stage
backend = HostedNeptuneBackend(Credentials())

project = backend.get_project('hubert3/sandbox')

n = 2
experiments = [backend.create_experiment(project.uuid) for _ in range(n)]

tempdir = TemporaryDirectory()
base_path = Path(tempdir.name)
experiment_paths = [base_path / str(exp.uuid) for exp in experiments]
for path in experiment_paths:
    path.mkdir()
    (path / 'operation-1.log').touch()
    last_operations_path = path / 'operations-2.log'
    last_operations = open(last_operations_path, 'w')
    last_operations.write('{"version":0,"operation":{}}{"version":1,"operation":{}}')
    last_operations.flush()
    last_operations.close()

experiment_offset = ExperimentOffset(experiment_paths[0])
experiment_offset.write(1)

experiment_offset = ExperimentOffset(experiment_paths[1])
experiment_offset.write(0)

print(list_experiments(base_path, *partition_experiments(base_path)))
