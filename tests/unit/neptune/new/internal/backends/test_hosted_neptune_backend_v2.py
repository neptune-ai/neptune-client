from unittest.mock import (
    Mock,
    patch,
)

import pytest
from neptune_api.credentials import Credentials
from neptune_api.models import (
    ClientConfig,
    ClientVersionsConfigDTO,
    ProjectDTO,
    SecurityDTO,
)

from neptune.internal.backends.hosted_neptune_backend_v2 import HostedNeptuneBackendV2

API_TOKEN = (
    "eyJhcGlfYWRkcmVzcyI6Imh0dHBzOi8vbXItMTI5MjcuZGV2Lm5lcHR1bmUuY"
    "WkiLCJhcGlfdXJsIjoiaHR0cHM6Ly9tci0xMjkyNy5kZXYubmVwdHVuZS5haSIsImFwaV"
    "9rZXkiOiJlNjg4MTk3ZS0xZGFmLTQ2YWEtYmYwNi05NzYxNjJkZGRlNjQifQ=="
)


@pytest.fixture
def credentials():
    return Credentials.from_api_key(API_TOKEN)


@pytest.fixture(autouse=True)
def neptune_api():
    config: ClientConfig = ClientConfig(
        api_url="api_url",
        py_lib_versions=ClientVersionsConfigDTO(
            min_recommended_version=None,
            min_compatible_version=None,
            max_compatible_version=None,
        ),
        security=SecurityDTO(
            client_id="client_id",
            open_id_discovery="open_id_discovery_url",
        ),
    )

    with (
        patch("neptune_api.api.backend.get_client_config.sync") as get_client_config_mock,
        patch("neptune_api.client.Client.get_httpx_client") as httpx_client_get_mock,
    ):
        get_client_config_mock.return_value = config
        httpx_client_get_mock.get.return_value = Mock(
            json=Mock(
                return_value={"token_endpoint": "token_endpoint", "authorization_endpoint": "authorization_endpoint"}
            )
        )
        yield


@pytest.fixture
def project_dto():
    yield ProjectDTO(
        name="project_name",
        organization_name="organization_name",
        organization_id="organization_id",
        id="project_id",
        project_key="project_key",
        version=3,
    )


def test_init(credentials):
    _ = HostedNeptuneBackendV2(credentials)


@patch("neptune_api.api.backend.get_project.sync_detailed")
def test_get_project(sync_detailed_mock, credentials, project_dto):
    sync_detailed_mock.return_value.parsed = project_dto
    backend = HostedNeptuneBackendV2(credentials)
    project = backend.get_project("project_name")
    assert project == project_dto
