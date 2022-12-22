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
__all__ = ["kill_me"]

import os
import signal

from neptune.new.envs import NEPTUNE_SUBPROCESS_KILL_TIMEOUT

try:
    import psutil

    PSUTIL_INSTALLED = True
except ImportError:
    PSUTIL_INSTALLED = False


KILL_TIMEOUT = int(os.getenv(NEPTUNE_SUBPROCESS_KILL_TIMEOUT, "5"))


def kill_me():
    if PSUTIL_INSTALLED:
        process = psutil.Process(os.getpid())
        try:
            children = _get_process_children(process)
        except psutil.NoSuchProcess:
            children = []

        for child_proc in children:
            _terminate(child_proc)
        _, alive = psutil.wait_procs(children, timeout=KILL_TIMEOUT)
        for child_proc in alive:
            _kill(child_proc)
        # finish with terminating self
        _terminate(process)
    else:
        os.kill(os.getpid(), signal.SIGINT)


def _terminate(process):
    try:
        process.terminate()
    except psutil.NoSuchProcess:
        pass


def _kill(process):
    try:
        if process.is_running():
            process.kill()
    except psutil.NoSuchProcess:
        pass


def _get_process_children(process):
    try:
        return process.children(recursive=True)
    except psutil.NoSuchProcess:
        return []
