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
from dataclasses import dataclass


@dataclass(frozen=True)
class MultipartConfig:
    min_chunk_size: int
    max_chunk_size: int
    max_chunk_count: int
    max_single_part_size: int

    @staticmethod
    def get_default() -> "MultipartConfig":
        return MultipartConfig(
            min_chunk_size=5242880,
            max_chunk_size=1073741824,
            max_chunk_count=1000,
            max_single_part_size=5242880,
        )
