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
from typing import Any, Iterable


def get_batches(iterable: Iterable[Any], batch_size: int):
    assert batch_size > 0
    iterable = iter(iterable)

    batch = list()
    while True:
        if len(batch) == batch_size:
            yield batch
            batch = list()

        try:
            next_element = next(iterable)
        except StopIteration:
            if batch:
                yield batch
            return

        batch.append(next_element)
