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
__all__ = ["BaseE2ETest", "AVAILABLE_CONTAINERS", "fake", "are_group_tags_enabled"]

import inspect
import os

from faker import Faker

fake = Faker()

AVAILABLE_CONTAINERS = ["project", "run", "model", "model_version"]


class BaseE2ETest:
    def gen_key(self):
        # Get test name
        caller_name = inspect.stack()[1][3]
        return f"{self.__class__.__name__}/{caller_name}/{fake.unique.slug()}"


NEPTUNE_GROUP_TAGS_ENABLED = "NEPTUNE_GROUP_TAGS_ENABLED"


def are_group_tags_enabled() -> bool:
    return os.getenv(NEPTUNE_GROUP_TAGS_ENABLED, "false").lower()[0] in ("t", "y", "1")
