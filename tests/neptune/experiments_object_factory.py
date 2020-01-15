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
import random

from munch import Munch

from tests.neptune.random_utils import a_uuid_string, a_string


def a_channel():
    x = random.randint(0, 100)
    return Munch(
        id=a_uuid_string(),
        name=a_string(),
        channelType='text',
        lastX=x,
        x=x,
        y=a_string()
    )
