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

import re


class GPUCardIndicesProvider(object):
    def __init__(self, cuda_visible_devices, gpu_card_count):
        self.__cuda_visible_devices = cuda_visible_devices
        self.__gpu_card_count = gpu_card_count
        self.__cuda_visible_devices_regex = r"^-?\d+(,-?\d+)*$"

    def get(self):
        if self.__is_cuda_visible_devices_correct():
            return self.__gpu_card_indices_from_cuda_visible_devices()
        else:
            return list(range(self.__gpu_card_count))

    def __is_cuda_visible_devices_correct(self):
        return self.__cuda_visible_devices is not None and re.match(
            self.__cuda_visible_devices_regex, self.__cuda_visible_devices
        )

    def __gpu_card_indices_from_cuda_visible_devices(self):
        correct_indices = []

        # According to CUDA Toolkit specification.
        # https://docs.nvidia.com/cuda/cuda-c-programming-guide/index.html#env-vars
        for gpu_index_str in self.__cuda_visible_devices.split(","):
            gpu_index = int(gpu_index_str)
            if 0 <= gpu_index < self.__gpu_card_count:
                correct_indices.append(gpu_index)
            else:
                break

        return list(set(correct_indices))
