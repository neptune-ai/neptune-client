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
__all__ = ["expect_not_an_experiment", "join_paths", "verify_type", "RunType"]

from typing import Union

from neptune.common.experiments import Experiment
from neptune.new import Run
from neptune.new.exceptions import NeptuneLegacyIncompatibilityException
from neptune.new.handler import Handler
from neptune.new.internal.utils import verify_type
from neptune.new.internal.utils.paths import join_paths


def expect_not_an_experiment(run: Run):
    if isinstance(run, Experiment):
        raise NeptuneLegacyIncompatibilityException()


RunType = Union[Run, Handler]
