import os
import pathlib


class DockerChecker(object):
    @staticmethod
    def runing_in_docker() -> bool:
        path = pathlib.Path("/proc/self/cgroup")
        return os.path.exists("/.dockerenv") or path.is_file() and any("docker" in line for line in path.read_text())
