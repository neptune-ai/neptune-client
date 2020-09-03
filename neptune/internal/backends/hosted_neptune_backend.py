#
# Copyright (c) 2020, Neptune Labs Sp. z o.o.
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
import platform
import uuid

from itertools import groupby
from typing import List, Optional, Dict, Any
from uuid import UUID

import urllib3

from bravado.client import SwaggerClient
from bravado.exception import HTTPNotFound
from bravado.requests_client import RequestsClient
from packaging import version

from neptune_old.internal.hardware.metrics.reports.metric_report import MetricReport
from neptune_old.internal.hardware.metrics.metric import Metric

from neptune.envs import NEPTUNE_ALLOW_SELF_SIGNED_CERTIFICATE
from neptune.exceptions import UnsupportedClientVersion, ProjectNotFound, ExperimentUUIDNotFound
from neptune.internal.backends.api_model import ClientConfig, Project, Experiment
from neptune.internal.backends.neptune_backend import NeptuneBackend
from neptune.internal.backends.utils import with_api_exceptions_handler, verify_host_resolution, \
    create_swagger_client, verify_client_version, update_session_proxies
from neptune.internal.credentials import Credentials
from neptune.internal.operation import Operation
from neptune.internal.utils import verify_type
from neptune.types.value import Value
from neptune.version import version as neptune_client_version
from neptune_old.oauth import NeptuneAuthenticator


class HostedNeptuneBackend(NeptuneBackend):
    BACKEND_SWAGGER_PATH = "/api/backend/swagger.json"
    LB_SWAGGER_PATH = "/api/leaderboard/swagger.json"

    @with_api_exceptions_handler
    def __init__(self, credentials: Credentials, proxies: Optional[Dict[str, str]] = None):
        self.credentials = credentials

        ssl_verify = True
        if os.getenv(NEPTUNE_ALLOW_SELF_SIGNED_CERTIFICATE):
            urllib3.disable_warnings()
            ssl_verify = False

        self._http_client = self._create_http_client(ssl_verify, proxies)

        config_api_url = self.credentials.api_url_opt or self.credentials.token_origin_address
        if proxies is None:
            verify_host_resolution(config_api_url)

        self.backend_client = create_swagger_client(config_api_url + self.BACKEND_SWAGGER_PATH, self._http_client)
        self._client_config = self._get_client_config(self.backend_client)
        verify_client_version(self._client_config, neptune_client_version)

        if config_api_url != self._client_config.api_url:
            self.backend_client = create_swagger_client(config_api_url + self.BACKEND_SWAGGER_PATH, self._http_client)
        self.leaderboard_client = create_swagger_client(config_api_url + self.LB_SWAGGER_PATH, self._http_client)

        # TODO: Do not use NeptuneAuthenticator from old_neptune. Move it to new package.
        self._http_client.authenticator = NeptuneAuthenticator(self._get_auth_tokens(), ssl_verify, proxies)

        user_agent = 'neptune-client/{lib_version} ({system}, python {python_version})'.format(
            lib_version=neptune_client_version,
            system=platform.platform(),
            python_version=platform.python_version())
        self._http_client.session.headers.update({'User-Agent': user_agent})

    def get_display_address(self) -> str:
        return self._client_config.display_url

    @with_api_exceptions_handler
    def get_project(self, project_id: str) -> Project:
        verify_type("project_id", project_id, str)

        try:
            project = self.backend_client.api.getProject(projectIdentifier=project_id).response().result
            return Project(uuid.UUID(project.id), project.name, project.organizationName)
        except HTTPNotFound:
            raise ProjectNotFound(project_id)

    @with_api_exceptions_handler
    def create_experiment(self,
                          project_uuid: uuid.UUID,
                          notebook_uuid: Optional[uuid.UUID] = None,
                          checkpoint_uuid: Optional[uuid.UUID] = None
                          ) -> Experiment:
        verify_type("project_uuid", project_uuid, uuid.UUID)

        params = {
            "projectIdentifier": str(project_uuid),
            "notebookId": str(notebook_uuid) if notebook_uuid is not None else None,
            "checkpointId": str(checkpoint_uuid) if checkpoint_uuid is not None else None,
            "name": "Untitled",
            "parameters": [],
            "properties": [],
            "tags": [],
        }

        kwargs = {
            'experimentCreationParams': params,
            'X-Neptune-CliVersion': str(neptune_client_version)
        }

        try:
            experiment = self.leaderboard_client.api.createExperiment(**kwargs).response().result
            return Experiment(uuid.UUID(experiment.id), experiment.shortId, project_uuid)
        except HTTPNotFound:
            raise ProjectNotFound(project_id=project_uuid)

    @with_api_exceptions_handler
    def execute_operations(self, operations: List[Operation]) -> None:
        pass

    @with_api_exceptions_handler
    def get(self, _uuid: uuid.UUID, path: List[str]) -> Value:
        pass

    @with_api_exceptions_handler
    def send_hardware_metric_reports(
            self,
            experiment_uuid: UUID,
            metrics: List[Metric],
            metric_reports: List[MetricReport]) -> None:
        verify_type("experiment_uuid", experiment_uuid, uuid.UUID)
        verify_type("metrics", metrics, list)
        verify_type("metric_reports", metric_reports, list)

        try:
            metrics_by_name = {metric.name: metric for metric in metrics}

            system_metric_values = [
                {
                    "metricId": metrics_by_name.get(report.metric.name).internal_id,
                    "seriesName": gauge_name,
                    "values": [
                        {
                            "timestampMillis": int(metric_value.timestamp * 1000.0),
                            "x": int(metric_value.running_time * 1000.0),
                            "y": metric_value.value
                        }
                        for metric_value in metric_values
                    ]
                }
                for report in metric_reports
                for gauge_name, metric_values in groupby(report.values, lambda value: value.gauge_name)
            ]

            self.backend_client.api.postSystemMetricValues(
                experimentId=experiment_uuid,
                metricValues=system_metric_values
            ).response()
        except HTTPNotFound:
            raise ExperimentUUIDNotFound(experiment_uuid)

    @with_api_exceptions_handler
    def create_hardware_metric(self, experiment_uuid: UUID, exec_id: str, metric: Metric) -> UUID:
        verify_type("experiment_uuid", experiment_uuid, uuid.UUID)
        verify_type("exec_id", exec_id, str)
        verify_type("metric", metric, Metric)

        try:
            series = [gauge.name() for gauge in metric.gauges]
            system_metric_params = {
                "name": "{} ({})".format(metric.name, exec_id),
                "description": metric.description,
                "resourceType": metric.resource_type,
                "unit": metric.unit,
                "min": metric.min_value,
                "max": metric.max_value,
                "series": series
            }

            metric_dto = self.backend_client.api.createSystemMetric(
                experimentId=experiment_uuid, metricToCreate=system_metric_params
            ).response().result

            return metric_dto.id
        except HTTPNotFound:
            raise ExperimentUUIDNotFound(experiment_uuid)

    @with_api_exceptions_handler
    def _get_client_config(self, backend_client: SwaggerClient) -> ClientConfig:
        config = backend_client.api.getClientConfig(X_Neptune_Api_Token=self.credentials.api_token).response().result

        if hasattr(config, "pyLibVersions"):
            min_recommended = getattr(config.pyLibVersions, "minRecommendedVersion", None)
            min_compatible = getattr(config.pyLibVersions, "minCompatibleVersion", None)
            max_compatible = getattr(config.pyLibVersions, "maxCompatibleVersion", None)
        else:
            raise UnsupportedClientVersion(neptune_client_version, max_version="0.4.111")

        return ClientConfig(
            api_url=config.apiUrl,
            display_url=config.applicationUrl,
            min_recommended_version=version.parse(min_recommended) if min_recommended else None,
            min_compatible_version=version.parse(min_compatible) if min_compatible else None,
            max_compatible_version=version.parse(max_compatible) if max_compatible else None
        )

    @with_api_exceptions_handler
    def create_checkpoint(self, notebook_id: uuid.UUID, jupyter_path: str) -> uuid.UUID:
        return UUID(self.leaderboard_client.api.createEmptyCheckpoint(
            notebookId=notebook_id,
            checkpoint={
                'path': jupyter_path
            }
        ).response().result.id)

    @staticmethod
    def _create_http_client(ssl_verify: bool, proxies: Dict[str, str]) -> RequestsClient:
        http_client = RequestsClient(ssl_verify=ssl_verify)
        update_session_proxies(http_client.session, proxies)
        return http_client

    @with_api_exceptions_handler
    def _get_auth_tokens(self) -> dict:
        return self.backend_client.api.exchangeApiToken(
            X_Neptune_Api_Token=self.credentials.api_token
        ).response().result
