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
import abc


class CGroupAbstractFilesystemReader(abc.ABC):
    NO_LIMIT_VALUE = "max"  # no limit special value in cgroup2

    @abc.abstractmethod
    def get_memory_usage_in_bytes(self):
        pass

    @abc.abstractmethod
    def get_memory_limit_in_bytes(self):
        pass

    @abc.abstractmethod
    def get_cpu_max_limits(self):
        pass

    @abc.abstractmethod
    def get_cpuacct_usage_nanos(self):
        pass
