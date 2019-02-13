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

import os


class CGroupFilesystemReader(object):
    def __init__(self):
        cgroup_memory_dir = self.__cgroup_mount_dir(subsystem='memory')
        self.__memory_usage_file = os.path.join(cgroup_memory_dir, 'memory.usage_in_bytes')
        self.__memory_limit_file = os.path.join(cgroup_memory_dir, 'memory.limit_in_bytes')

        cgroup_cpu_dir = self.__cgroup_mount_dir(subsystem='cpuacct')
        self.__cpu_period_file = os.path.join(cgroup_cpu_dir, 'cpu.cfs_period_us')
        self.__cpu_quota_file = os.path.join(cgroup_cpu_dir, 'cpu.cfs_quota_us')
        self.__cpuacct_usage_file = os.path.join(cgroup_cpu_dir, 'cpuacct.usage')

    def get_memory_usage_in_bytes(self):
        return self.__read_int_file(self.__memory_usage_file)

    def get_memory_limit_in_bytes(self):
        return self.__read_int_file(self.__memory_limit_file)

    def get_cpu_quota_micros(self):
        return self.__read_int_file(self.__cpu_quota_file)

    def get_cpu_period_micros(self):
        return self.__read_int_file(self.__cpu_period_file)

    def get_cpuacct_usage_nanos(self):
        return self.__read_int_file(self.__cpuacct_usage_file)

    def __read_int_file(self, filename):
        with open(filename) as f:
            return int(f.read())

    def __cgroup_mount_dir(self, subsystem):
        """
        :param subsystem: cgroup subsystem like memory, cpu
        :return: directory where given subsystem is mounted
        """
        with open('/proc/mounts', 'r') as f:
            for l in f.readlines():
                if 'cgroup' in l and subsystem in l:
                    return l.split()[1]

        assert False, 'Mount directory for "{}" subsystem not found'.format(subsystem)
