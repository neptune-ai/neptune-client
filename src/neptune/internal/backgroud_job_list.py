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
__all__ = ["BackgroundJobList"]

import time
from typing import (
    TYPE_CHECKING,
    List,
    Optional,
)

from neptune.internal.background_job import BackgroundJob

if TYPE_CHECKING:
    from neptune.metadata_containers import MetadataContainer


class BackgroundJobList(BackgroundJob):
    def __init__(self, jobs: List[BackgroundJob]):
        self._jobs = jobs

    def start(self, container: "MetadataContainer"):
        for job in self._jobs:
            job.start(container)

    def stop(self):
        for job in self._jobs:
            job.stop()

    def join(self, seconds: Optional[float] = None):
        ts = time.time()
        for job in self._jobs:
            sec_left = None if seconds is None else seconds - (time.time() - ts)
            job.join(sec_left)

    def pause(self):
        for job in self._jobs:
            job.pause()

    def resume(self):
        for job in self._jobs:
            job.resume()
