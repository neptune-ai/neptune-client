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

import random
import string
import uuid
from random import randint

from neptune.new.internal.backends.api_model import ApiExperiment
from neptune.new.internal.container_type import ContainerType
from neptune.new.internal.id_formats import SysId, UniqueId


def api_run():
    return _api_experiment(
        sys_id=f"{_random_key()}-{randint(42, 12342)}",
        container_type=ContainerType.RUN,
    )


def api_model():
    return _api_experiment(
        sys_id=f"{_random_key()}-{_random_key()}",
        container_type=ContainerType.MODEL,
    )


def api_model_version():
    return _api_experiment(
        sys_id=f"{_random_key()}-{_random_key()}-{randint(42, 12342)}",
        container_type=ContainerType.MODEL_VERSION,
    )


def api_project():
    return _api_experiment(
        sys_id=_random_key(),
        container_type=ContainerType.PROJECT,
    )


def _api_experiment(sys_id: str, container_type: ContainerType):
    return ApiExperiment(
        id=UniqueId(str(uuid.uuid4())),
        type=container_type,
        sys_id=SysId(sys_id),
        workspace="workspace",
        project_name="sandbox",
        trashed=False,
    )


def _random_key(n=3):
    return "".join((random.choice(string.ascii_letters).upper() for _ in range(n)))
