import os
import signal
import unittest
from multiprocessing import Barrier

import pytest

from neptune.common.utils import IS_WINDOWS
from tests.e2e.base import AVAILABLE_CONTAINERS
from tests.e2e.utils import (
    Environment,
    initialize_container,
)


@unittest.skipIf(IS_WINDOWS, "Windows does not support fork")
@pytest.mark.parametrize("container_type", AVAILABLE_CONTAINERS)
def test_fork_child_parent_info_exchange(container_type: str, environment: Environment):
    barrier = Barrier(2)
    with initialize_container(container_type=container_type, project=environment.project) as container:
        child_pid = os.fork()
        if child_pid == 0:
            # child process exec
            container["child_key"] = "child_value"
            container.wait()
            barrier.wait()  # after barrier both processes have sent data

            container.sync()
            assert container["parent_key"].fetch() == "parent_value"

            os.kill(os.getpid(), signal.SIGTERM)  # kill child process, as it has cloned testing runtime
        else:
            # parent process exec
            container["parent_key"] = "parent_value"
            container.wait()
            barrier.wait()  # after barrier both processes have sent data

            container.sync()
            assert container["child_key"].fetch() == "child_value"

            os.waitpid(child_pid, 0)
