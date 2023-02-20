import pytest

from neptune import init_project
from neptune.metadata_containers import Project


@pytest.fixture(scope="session")
def project() -> Project:
    return init_project(mode="read-only")
