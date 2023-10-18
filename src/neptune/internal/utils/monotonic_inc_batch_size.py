#
# Copyright (c) 2023, Neptune Labs Sp. z o.o.
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
class MonotonicIncBatchSize:
    def __init__(self, size_limit: int, initial_size: int = 10, scale_coef: float = 1.6):
        assert size_limit > 0
        assert scale_coef > 1
        assert 0 < initial_size <= size_limit

        self._size_limit: int = size_limit
        self._current_size: int = initial_size
        self._scale_coef: float = scale_coef

    def increase(self) -> None:
        self._current_size = min(int(self._current_size * self._scale_coef + 1), self._size_limit)

    def get(self) -> int:
        return self._current_size
