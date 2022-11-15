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
from pathlib import Path

import click

from neptune.new.cli.clear import ClearRunner
from neptune.new.cli.path_option import path_option
from neptune.new.internal.backends.hosted_neptune_backend import HostedNeptuneBackend
from neptune.new.internal.credentials import Credentials

__all__ = ["clear"]


@click.command()
@path_option
def clear(path: Path):
    """
    TODO NPT-12295
    """
    backend = HostedNeptuneBackend(Credentials.from_token())
    clear_runner = ClearRunner(backend=backend)

    clear_runner.clear(path)
