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
import logging
import os
import platform
import re
import uuid
from typing import List, Optional, Dict, Iterable

import click
import urllib3
from bravado.client import SwaggerClient
from bravado.exception import HTTPNotFound, HTTPUnprocessableEntity
from bravado.requests_client import RequestsClient
from packaging import version

from neptune.new.envs import NEPTUNE_ALLOW_SELF_SIGNED_CERTIFICATE
from neptune.new.exceptions import (
    ClientHttpError,
    FetchAttributeNotFoundException,
    RunNotFound,
    RunUUIDNotFound,
    InternalClientError,
    MetadataInconsistency,
    NeptuneException,
    NeptuneLegacyProjectException,
    ProjectNotFound,
    NeptuneStorageLimitException,
    UnsupportedClientVersion,
)
from neptune.new.internal.backends.api_model import (
    ApiRun,
    Attribute,
    AttributeType,
    AttributeWithProperties,
    BoolAttribute,
    ClientConfig,
    DatetimeAttribute,
    FileAttribute,
    FloatAttribute,
    FloatSeriesAttribute,
    IntAttribute,
    LeaderboardEntry,
    Project,
    StringAttribute,
    StringSeriesAttribute,
    StringSetAttribute,
    StringSeriesValues,
    StringPointValue,
    FloatSeriesValues,
    FloatPointValue,
    ImageSeriesValues,
)
from neptune.new.internal.backends.hosted_file_operations import (
    download_file_attribute,
    download_file_set_attribute,
    download_image_series_element,
    upload_file_attribute,
    upload_file_set_attribute,
)
from neptune.new.internal.backends.neptune_backend import NeptuneBackend
from neptune.new.internal.backends.operation_api_name_visitor import OperationApiNameVisitor
from neptune.new.internal.backends.operation_api_object_converter import OperationApiObjectConverter
from neptune.new.internal.backends.operations_preprocessor import OperationsPreprocessor
from neptune.new.internal.backends.utils import (
    create_swagger_client,
    update_session_proxies,
    verify_client_version,
    verify_host_resolution,
    with_api_exceptions_handler,
)
from neptune.new.internal.credentials import Credentials
from neptune.new.internal.operation import (
    Operation,
    UploadFile,
    UploadFileContent,
    UploadFileSet,
)
from neptune.new.internal.utils import verify_type, base64_decode
from neptune.new.internal.utils.paths import path_to_str
from neptune.new.internal.websockets.websockets_factory import WebsocketsFactory
from neptune.new.types.atoms import GitRef
from neptune.new.version import version as neptune_client_version
from neptune.oauth import NeptuneAuthenticator

_logger = logging.getLogger(__name__)


class HostedNeptuneBackend(NeptuneBackend):
    BACKEND_SWAGGER_PATH = "/api/backend/swagger.json"
    LEADERBOARD_SWAGGER_PATH = "/api/leaderboard/swagger.json"

    CONNECT_TIMEOUT = 30  # helps detecting internet connection lost
    REQUEST_TIMEOUT = None

    DEFAULT_REQUEST_KWARGS = {
        '_request_options': {
            "connect_timeout": CONNECT_TIMEOUT,
            "timeout": REQUEST_TIMEOUT,
        }
    }

    def __init__(self, credentials: Credentials, proxies: Optional[Dict[str, str]] = None):
        self.credentials = credentials
        self.proxies = proxies

        ssl_verify = True
        if os.getenv(NEPTUNE_ALLOW_SELF_SIGNED_CERTIFICATE):
            urllib3.disable_warnings()
            ssl_verify = False

        self._http_client = self._create_http_client(ssl_verify, proxies)

        config_api_url = self.credentials.api_url_opt or self.credentials.token_origin_address
        if proxies is None:
            verify_host_resolution(config_api_url)

        token_http_client = self._create_http_client(ssl_verify, proxies)
        token_client = create_swagger_client(config_api_url + self.BACKEND_SWAGGER_PATH,
                                             token_http_client)

        self._client_config = self._get_client_config(token_client)
        verify_client_version(self._client_config, neptune_client_version)

        if config_api_url != self._client_config.api_url:
            token_client = create_swagger_client(self._client_config.api_url + self.BACKEND_SWAGGER_PATH,
                                                 token_http_client)

        self.backend_client = create_swagger_client(self._client_config.api_url + self.BACKEND_SWAGGER_PATH,
                                                    self._http_client)
        self.leaderboard_client = create_swagger_client(self._client_config.api_url + self.LEADERBOARD_SWAGGER_PATH,
                                                        self._http_client)

        # TODO: Do not use NeptuneAuthenticator from old_neptune. Move it to new package.
        self._authenticator = NeptuneAuthenticator(
            self.credentials.api_token,
            token_client,
            ssl_verify,
            proxies)
        self._http_client.authenticator = self._authenticator

        user_agent = 'neptune-client/{lib_version} ({system}, python {python_version})'.format(
            lib_version=neptune_client_version,
            system=platform.platform(),
            python_version=platform.python_version())
        self._http_client.session.headers.update({'User-Agent': user_agent})

    def get_display_address(self) -> str:
        return self._client_config.display_url

    def websockets_factory(self, project_uuid: uuid.UUID, run_uuid: uuid.UUID) -> Optional[WebsocketsFactory]:
        base_url = re.sub(r'^http', 'ws', self._client_config.api_url)
        return WebsocketsFactory(
            url=base_url + f'/api/notifications/v1/runs/{str(project_uuid)}/{str(run_uuid)}/signal',
            session=self._authenticator.auth.session,
            proxies=self.proxies
        )

    @with_api_exceptions_handler
    def get_project(self, project_id: str) -> Project:
        verify_type("project_id", project_id, str)

        try:
            response = self.backend_client.api.getProject(
                projectIdentifier=project_id,
                **self.DEFAULT_REQUEST_KWARGS,
            ).response()
            warning = response.metadata.headers.get('X-Server-Warning')
            if warning:
                click.echo(warning)  # TODO print in color once colored exceptions are added
            project = response.result
            project_version = project.version if hasattr(project, 'version') else 1
            if project_version < 2:
                raise NeptuneLegacyProjectException(project_id)
            return Project(uuid.UUID(project.id), project.name, project.organizationName)
        except HTTPNotFound:
            raise ProjectNotFound(project_id)

    @with_api_exceptions_handler
    def get_run(self, run_id: str):
        try:
            run = self.leaderboard_client.api.getExperiment(
                experimentId=run_id,
                **self.DEFAULT_REQUEST_KWARGS,
            ).response().result
            return ApiRun(uuid.UUID(run.id), run.shortId, run.organizationName, run.projectName, run.trashed)
        except HTTPNotFound:
            raise RunNotFound(run_id)

    @with_api_exceptions_handler
    def create_run(self,
                   project_uuid: uuid.UUID,
                   git_ref: Optional[GitRef] = None,
                   custom_run_id: Optional[str] = None,
                   notebook_id: Optional[uuid.UUID] = None,
                   checkpoint_id: Optional[uuid.UUID] = None
                   ) -> ApiRun:
        verify_type("project_uuid", project_uuid, uuid.UUID)

        git_info = {
            "commit": {
                "commitId": git_ref.commit_id,
                "message": git_ref.message,
                "authorName": git_ref.author_name,
                "authorEmail": git_ref.author_email,
                "commitDate": git_ref.commit_date
            },
            "repositoryDirty": git_ref.dirty,
            "currentBranch": git_ref.branch,
            "remotes": git_ref.remotes
        } if git_ref else None

        params = {
            "projectIdentifier": str(project_uuid),
            "cliVersion": str(neptune_client_version),
            "gitInfo": git_info,
            "customId": custom_run_id,
        }

        if notebook_id is not None and checkpoint_id is not None:
            params["notebookId"] = str(notebook_id) if notebook_id is not None else None
            params["checkpointId"] = str(checkpoint_id) if checkpoint_id is not None else None

        kwargs = {
            'experimentCreationParams': params,
            'X-Neptune-CliVersion': str(neptune_client_version),
            **self.DEFAULT_REQUEST_KWARGS,
        }

        try:
            run = self.leaderboard_client.api.createExperiment(**kwargs).response().result
            return ApiRun(uuid.UUID(run.id), run.shortId, run.organizationName, run.projectName, run.trashed)
        except HTTPNotFound:
            raise ProjectNotFound(project_id=project_uuid)

    @with_api_exceptions_handler
    def create_checkpoint(self, notebook_id: uuid.UUID, jupyter_path: str) -> Optional[uuid.UUID]:
        try:
            return self.leaderboard_client.api.createEmptyCheckpoint(
                notebookId=notebook_id,
                checkpoint={
                    "path": jupyter_path
                },
                **self.DEFAULT_REQUEST_KWARGS,
            ).response().result.id
        except HTTPNotFound:
            return None

    @with_api_exceptions_handler
    def ping_run(self, run_uuid: uuid.UUID):
        request_kwargs = {
            "_request_options": {
                "timeout": 10, "connect_timeout": 10,
            }
        }
        try:
            self.leaderboard_client.api.ping(
                experimentId=str(run_uuid),
                **request_kwargs,
            ).response().result
        except HTTPNotFound:
            raise RunUUIDNotFound(run_uuid)

    def execute_operations(self, run_uuid: uuid.UUID, operations: List[Operation]) -> List[NeptuneException]:
        errors = []

        operations_preprocessor = OperationsPreprocessor()
        operations_preprocessor.process(operations)
        errors.extend(operations_preprocessor.get_errors())

        upload_operations, other_operations = [], []
        file_operations = (UploadFile, UploadFileContent, UploadFileSet)
        for op in operations_preprocessor.get_operations():
            (upload_operations if isinstance(op, file_operations) else other_operations).append(op)

        # Upload operations should be done first since they are idempotent
        errors.extend(
            self._execute_upload_operations(run_uuid=run_uuid,
                                            upload_operations=upload_operations)
        )

        if other_operations:
            errors.extend(self._execute_operations(run_uuid, other_operations))

        return errors

    def _execute_upload_operations(self,
                                   run_uuid: uuid.UUID,
                                   upload_operations: List[Operation]) -> List[NeptuneException]:
        errors = list()

        for op in upload_operations:
            if isinstance(op, UploadFile):
                error = upload_file_attribute(
                    swagger_client=self.leaderboard_client,
                    run_uuid=run_uuid,
                    attribute=path_to_str(op.path),
                    source=op.file_path,
                    ext=op.ext)
                if error:
                    errors.append(error)
            elif isinstance(op, UploadFileContent):
                error = upload_file_attribute(
                    swagger_client=self.leaderboard_client,
                    run_uuid=run_uuid,
                    attribute=path_to_str(op.path),
                    source=base64_decode(op.file_content),
                    ext=op.ext)
                if error:
                    errors.append(error)
            elif isinstance(op, UploadFileSet):
                error = upload_file_set_attribute(
                    swagger_client=self.leaderboard_client,
                    run_uuid=run_uuid,
                    attribute=path_to_str(op.path),
                    file_globs=op.file_globs,
                    reset=op.reset)
                if error:
                    errors.append(error)
            else:
                raise InternalClientError("Upload operation in neither File or FileSet")

        return errors

    @with_api_exceptions_handler
    def _execute_operations(self,
                            run_uuid: uuid.UUID,
                            operations: List[Operation]) -> List[MetadataInconsistency]:
        kwargs = {
            'experimentId': str(run_uuid),
            'operations': [{
                'path': path_to_str(op.path),
                OperationApiNameVisitor().visit(op): OperationApiObjectConverter().convert(op)
            } for op in operations],
            **self.DEFAULT_REQUEST_KWARGS,
        }

        try:
            result = self.leaderboard_client.api.executeOperations(**kwargs).response().result
            return [MetadataInconsistency(err.errorDescription) for err in result]
        except HTTPNotFound as e:
            raise RunUUIDNotFound(run_uuid=run_uuid) from e
        except HTTPUnprocessableEntity:
            raise NeptuneStorageLimitException()

    @with_api_exceptions_handler
    def get_attributes(self, run_uuid: uuid.UUID) -> List[Attribute]:
        def to_attribute(attr) -> Attribute:
            return Attribute(attr.name, AttributeType(attr.type))

        params = {
            'experimentId': str(run_uuid),
            **self.DEFAULT_REQUEST_KWARGS,
        }
        try:
            run = self.leaderboard_client.api.getExperimentAttributes(**params).response().result

            attribute_type_names = [at.value for at in AttributeType]
            accepted_attributes = [attr for attr in run.attributes if attr.type in attribute_type_names]

            # Notify about ignored attrs
            ignored_attributes = \
                set(attr.type for attr in run.attributes) - set(attr.type for attr in accepted_attributes)
            if ignored_attributes:
                _logger.warning(
                    f"Ignored following attributes (unknown type): {ignored_attributes}.\n"
                    f"Try to upgrade `neptune-client."
                )

            return [to_attribute(attr) for attr in accepted_attributes if attr.type in attribute_type_names]
        except HTTPNotFound:
            raise RunUUIDNotFound(run_uuid=run_uuid)

    def download_file_series_by_index(self, run_uuid: uuid.UUID, path: List[str],
                                      index: int, destination: str):
        try:
            download_image_series_element(
                swagger_client=self.leaderboard_client,
                run_uuid=run_uuid,
                attribute=path_to_str(path),
                index=index,
                destination=destination
            )
        except ClientHttpError as e:
            if e.status == HTTPNotFound.status_code:
                raise FetchAttributeNotFoundException(path_to_str(path))
            else:
                raise

    def download_file(self, run_uuid: uuid.UUID, path: List[str], destination: Optional[str] = None):
        try:
            download_file_attribute(
                swagger_client=self.leaderboard_client,
                run_uuid=run_uuid,
                attribute=path_to_str(path),
                destination=destination)
        except ClientHttpError as e:
            if e.status == HTTPNotFound.status_code:
                raise FetchAttributeNotFoundException(path_to_str(path))
            else:
                raise

    def download_file_set(self, run_uuid: uuid.UUID, path: List[str], destination: Optional[str] = None):
        download_request = self._get_file_set_download_request(run_uuid, path)
        try:
            download_file_set_attribute(
                swagger_client=self.leaderboard_client,
                download_id=download_request.id,
                destination=destination)
        except ClientHttpError as e:
            if e.status == HTTPNotFound.status_code:
                raise FetchAttributeNotFoundException(path_to_str(path))
            else:
                raise

    @with_api_exceptions_handler
    def get_float_attribute(self, run_uuid: uuid.UUID, path: List[str]) -> FloatAttribute:
        params = {
            'experimentId': str(run_uuid),
            'attribute': path_to_str(path),
            **self.DEFAULT_REQUEST_KWARGS,
        }
        try:
            result = self.leaderboard_client.api.getFloatAttribute(**params).response().result
            return FloatAttribute(result.value)
        except HTTPNotFound:
            raise FetchAttributeNotFoundException(path_to_str(path))

    @with_api_exceptions_handler
    def get_int_attribute(self, run_uuid: uuid.UUID, path: List[str]) -> IntAttribute:
        params = {
            'experimentId': str(run_uuid),
            'attribute': path_to_str(path),
            **self.DEFAULT_REQUEST_KWARGS,
        }
        try:
            result = self.leaderboard_client.api.getIntAttribute(**params).response().result
            return IntAttribute(result.value)
        except HTTPNotFound:
            raise FetchAttributeNotFoundException(path_to_str(path))

    @with_api_exceptions_handler
    def get_bool_attribute(self, run_uuid: uuid.UUID, path: List[str]) -> BoolAttribute:
        params = {
            'experimentId': str(run_uuid),
            'attribute': path_to_str(path),
            **self.DEFAULT_REQUEST_KWARGS,
        }
        try:
            result = self.leaderboard_client.api.getBoolAttribute(**params).response().result
            return BoolAttribute(result.value)
        except HTTPNotFound:
            raise FetchAttributeNotFoundException(path_to_str(path))

    @with_api_exceptions_handler
    def get_file_attribute(self, run_uuid: uuid.UUID, path: List[str]) -> FileAttribute:
        params = {
            'experimentId': str(run_uuid),
            'attribute': path_to_str(path),
            **self.DEFAULT_REQUEST_KWARGS,
        }
        try:
            result = self.leaderboard_client.api.getFileAttribute(**params).response().result
            return FileAttribute(name=result.name, ext=result.ext, size=result.size)
        except HTTPNotFound:
            raise FetchAttributeNotFoundException(path_to_str(path))

    @with_api_exceptions_handler
    def get_string_attribute(self, run_uuid: uuid.UUID, path: List[str]) -> StringAttribute:
        params = {
            'experimentId': str(run_uuid),
            'attribute': path_to_str(path),
            **self.DEFAULT_REQUEST_KWARGS,
        }
        try:
            result = self.leaderboard_client.api.getStringAttribute(**params).response().result
            return StringAttribute(result.value)
        except HTTPNotFound:
            raise FetchAttributeNotFoundException(path_to_str(path))

    @with_api_exceptions_handler
    def get_datetime_attribute(self, run_uuid: uuid.UUID, path: List[str]) -> DatetimeAttribute:
        params = {
            'experimentId': str(run_uuid),
            'attribute': path_to_str(path),
            **self.DEFAULT_REQUEST_KWARGS,
        }
        try:
            result = self.leaderboard_client.api.getDatetimeAttribute(**params).response().result
            return DatetimeAttribute(result.value)
        except HTTPNotFound:
            raise FetchAttributeNotFoundException(path_to_str(path))

    @with_api_exceptions_handler
    def get_float_series_attribute(self, run_uuid: uuid.UUID, path: List[str]) -> FloatSeriesAttribute:
        params = {
            'experimentId': str(run_uuid),
            'attribute': path_to_str(path),
            **self.DEFAULT_REQUEST_KWARGS,
        }
        try:
            result = self.leaderboard_client.api.getFloatSeriesAttribute(**params).response().result
            return FloatSeriesAttribute(result.last)
        except HTTPNotFound:
            raise FetchAttributeNotFoundException(path_to_str(path))

    @with_api_exceptions_handler
    def get_string_series_attribute(self, run_uuid: uuid.UUID, path: List[str]) -> StringSeriesAttribute:
        params = {
            'experimentId': str(run_uuid),
            'attribute': path_to_str(path),
            **self.DEFAULT_REQUEST_KWARGS,
        }
        try:
            result = self.leaderboard_client.api.getStringSeriesAttribute(**params).response().result
            return StringSeriesAttribute(result.last)
        except HTTPNotFound:
            raise FetchAttributeNotFoundException(path_to_str(path))

    @with_api_exceptions_handler
    def get_string_set_attribute(self, run_uuid: uuid.UUID, path: List[str]) -> StringSetAttribute:
        params = {
            'experimentId': str(run_uuid),
            'attribute': path_to_str(path),
            **self.DEFAULT_REQUEST_KWARGS,
        }
        try:
            result = self.leaderboard_client.api.getStringSetAttribute(**params).response().result
            return StringSetAttribute(set(result.values))
        except HTTPNotFound:
            raise FetchAttributeNotFoundException(path_to_str(path))

    @with_api_exceptions_handler
    def get_image_series_values(self, run_uuid: uuid.UUID, path: List[str],
                                offset: int, limit: int) -> ImageSeriesValues:
        params = {
            'experimentId': str(run_uuid),
            'attribute': path_to_str(path),
            'limit': limit,
            'offset': offset,
            **self.DEFAULT_REQUEST_KWARGS,
        }
        try:
            result = self.leaderboard_client.api.getImageSeriesValues(**params).response().result
            return ImageSeriesValues(result.totalItemCount)
        except HTTPNotFound:
            raise FetchAttributeNotFoundException(path_to_str(path))

    @with_api_exceptions_handler
    def get_string_series_values(self, run_uuid: uuid.UUID, path: List[str],
                                 offset: int, limit: int) -> StringSeriesValues:
        params = {
            'experimentId': str(run_uuid),
            'attribute': path_to_str(path),
            'limit': limit,
            'offset': offset,
            **self.DEFAULT_REQUEST_KWARGS,
        }
        try:
            result = self.leaderboard_client.api.getStringSeriesValues(**params).response().result
            return StringSeriesValues(result.totalItemCount,
                                      [StringPointValue(v.timestampMillis, v.step, v.value) for v in result.values])
        except HTTPNotFound:
            raise FetchAttributeNotFoundException(path_to_str(path))

    @with_api_exceptions_handler
    def get_float_series_values(self, run_uuid: uuid.UUID, path: List[str],
                                offset: int, limit: int) -> FloatSeriesValues:
        params = {
            'experimentId': str(run_uuid),
            'attribute': path_to_str(path),
            'limit': limit,
            'offset': offset,
            **self.DEFAULT_REQUEST_KWARGS,
        }
        try:
            result = self.leaderboard_client.api.getFloatSeriesValues(**params).response().result
            return FloatSeriesValues(result.totalItemCount,
                                     [FloatPointValue(v.timestampMillis, v.step, v.value) for v in result.values])
        except HTTPNotFound:
            raise FetchAttributeNotFoundException(path_to_str(path))

    @with_api_exceptions_handler
    def _get_file_set_download_request(self, run_uuid: uuid.UUID, path: List[str]):
        params = {
            'experimentId': str(run_uuid),
            'attribute': path_to_str(path),
            **self.DEFAULT_REQUEST_KWARGS,
        }
        try:
            return self.leaderboard_client.api.prepareForDownloadFileSetAttributeZip(**params).response().result
        except HTTPNotFound:
            raise FetchAttributeNotFoundException(path_to_str(path))

    @with_api_exceptions_handler
    def _get_client_config(self, backend_client: SwaggerClient) -> ClientConfig:
        config = backend_client.api.getClientConfig(
            X_Neptune_Api_Token=self.credentials.api_token,
            alpha="true",
            **self.DEFAULT_REQUEST_KWARGS,
        ).response().result

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
    def get_leaderboard(self, project_id: uuid.UUID,
                        _id: Optional[Iterable[str]] = None,
                        state: Optional[Iterable[str]] = None,
                        owner: Optional[Iterable[str]] = None,
                        tags: Optional[Iterable[str]] = None
                        ) -> List[LeaderboardEntry]:

        def get_portion(limit, offset):
            return self.leaderboard_client.api.getLeaderboard(
                projectIdentifier=str(project_id),
                shortId=_id, state=state, owner=owner, tags=tags, tagsMode='and',
                sortBy=['shortId'], sortFieldType=['string'], sortDirection=['ascending'],
                limit=limit, offset=offset,
                **self.DEFAULT_REQUEST_KWARGS,
            ).response().result.entries

        def to_leaderboard_entry(entry) -> LeaderboardEntry:
            supported_attribute_types = {item.value for item in AttributeType}
            attributes: List[AttributeWithProperties] = []
            for attr in entry.attributes:
                if attr.type in supported_attribute_types:
                    properties = attr.__getitem__("{}Properties".format(attr.type))
                    attributes.append(AttributeWithProperties(
                        attr.name,
                        AttributeType(attr.type),
                        properties
                    ))
            return LeaderboardEntry(entry.id, attributes)

        try:
            return [to_leaderboard_entry(e) for e in self._get_all_items(get_portion, step=100)]
        except HTTPNotFound:
            raise ProjectNotFound(project_id)

    def get_run_url(self, run_uuid: uuid.UUID, workspace: str, project_name: str, short_id: str) -> str:
        base_url = self.get_display_address()
        return f"{base_url}/{workspace}/{project_name}/e/{short_id}"

    @staticmethod
    def _create_http_client(ssl_verify: bool, proxies: Dict[str, str]) -> RequestsClient:
        http_client = RequestsClient(ssl_verify=ssl_verify)
        http_client.session.verify = ssl_verify
        update_session_proxies(http_client.session, proxies)
        return http_client

    @staticmethod
    def _get_all_items(get_portion, step):
        items = []
        previous_items = None
        while previous_items is None or len(previous_items) >= step:
            previous_items = get_portion(limit=step, offset=len(items))
            items += previous_items
        return items
