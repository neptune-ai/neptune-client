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


def file_contains(filename, text):
    with open(filename) as f:
        for line in f:
            if text in line:
                return True
        return False


def in_docker():
    cgroup_file = "/proc/self/cgroup"
    return os.path.exists("./dockerenv") or (os.path.exists(cgroup_file) and file_contains(cgroup_file, text="docker"))


def is_ipython():
    try:
        # pylint:disable=bad-option-value,import-outside-toplevel
        import IPython

        ipython = IPython.core.getipython.get_ipython()
        return ipython is not None
    except ImportError:
        return False
