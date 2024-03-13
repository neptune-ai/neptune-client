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


class SystemResourceInfo(object):
    def __init__(
        self,
        cpu_core_count,
        memory_amount_bytes,
        gpu_card_indices,
        gpu_memory_amount_bytes,
    ):
        self.__cpu_core_count = cpu_core_count
        self.__memory_amount_bytes = memory_amount_bytes
        self.__gpu_card_indices = gpu_card_indices
        self.__gpu_memory_amount_bytes = gpu_memory_amount_bytes

    @property
    def cpu_core_count(self):
        return self.__cpu_core_count

    @property
    def memory_amount_bytes(self):
        return self.__memory_amount_bytes

    @property
    def gpu_card_count(self):
        return len(self.__gpu_card_indices)

    @property
    def gpu_card_indices(self):
        return self.__gpu_card_indices

    @property
    def gpu_memory_amount_bytes(self):
        return self.__gpu_memory_amount_bytes

    def has_gpu(self):
        return self.gpu_card_count > 0

    def __repr__(self):
        return str(self.__dict__)
