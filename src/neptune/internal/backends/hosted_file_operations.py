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
__all__ = [
    "upload_file_attribute",
    "upload_file_set_attribute",
    "get_unique_upload_entries",
    "download_file_set_attribute",
]

import collections
import enum
import json
import os
import time
from contextlib import ExitStack
from io import BytesIO
from typing import (
    AnyStr,
    Dict,
    Iterable,
    List,
    Optional,
    Set,
    Union,
)
from urllib.parse import urlencode

from bravado.exception import (
    HTTPPaymentRequired,
    HTTPUnprocessableEntity,
)
from bravado.requests_client import RequestsClient
from requests import (
    Request,
    Response,
)

from neptune.common.backends.api_model import MultipartConfig
from neptune.common.backends.utils import with_api_exceptions_handler
from neptune.common.exceptions import (
    InternalClientError,
    NeptuneException,
    UploadedFileChanged,
)
from neptune.common.hardware.constants import BYTES_IN_ONE_MB
from neptune.common.storage.datastream import (
    FileChunk,
    FileChunker,
    compress_to_tar_gz_in_memory,
)
from neptune.common.storage.storage_utils import (
    AttributeUploadConfiguration,
    UploadEntry,
    normalize_file_name,
    scan_unique_upload_entries,
    split_upload_files,
)
from neptune.exceptions import (
    FileUploadError,
    MetadataInconsistency,
    NeptuneLimitExceedException,
)
from neptune.internal.backends.swagger_client_wrapper import (
    ApiMethodWrapper,
    SwaggerClientWrapper,
)
from neptune.internal.backends.utils import (
    build_operation_url,
    construct_progress_bar,
    handle_server_raw_response_messages,
)
from neptune.internal.utils import (
    get_absolute_paths,
    get_common_root,
)
from neptune.internal.utils.logger import get_logger
from neptune.typing import ProgressBarType

logger = get_logger()
DEFAULT_CHUNK_SIZE = 5 * BYTES_IN_ONE_MB
DEFAULT_UPLOAD_CONFIG = AttributeUploadConfiguration(chunk_size=DEFAULT_CHUNK_SIZE)


class FileUploadTarget(enum.Enum):
    FILE_ATOM = "file"
    FILE_SET = "fileset"


def upload_file_attribute(
    swagger_client: SwaggerClientWrapper,
    container_id: str,
    attribute: str,
    source: Union[str, bytes],
    ext: str,
    multipart_config: Optional[MultipartConfig],
) -> List[NeptuneException]:
    if isinstance(source, str) and not os.path.isfile(source):
        return [FileUploadError(source, "Path not found or is a not a file.")]

    target = attribute
    if ext:
        target += "." + ext

    try:
        upload_entry = UploadEntry(source if isinstance(source, str) else BytesIO(source), target)
        _multichunk_upload_with_retry(
            upload_entry,
            query_params={
                "experimentIdentifier": container_id,
                "attribute": attribute,
                "ext": ext,
            },
            swagger_client=swagger_client,
            multipart_config=multipart_config,
            target=FileUploadTarget.FILE_ATOM,
        )
    except MetadataInconsistency as e:
        return [e]


def upload_file_set_attribute(
    swagger_client: SwaggerClientWrapper,
    container_id: str,
    attribute: str,
    file_globs: Iterable[str],
    reset: bool,
    multipart_config: Optional[MultipartConfig],
) -> List[NeptuneException]:
    unique_upload_entries = get_unique_upload_entries(file_globs)

    try:
        upload_configuration = DEFAULT_UPLOAD_CONFIG
        for package in split_upload_files(
            upload_entries=unique_upload_entries,
            upload_configuration=upload_configuration,
        ):
            if package.is_empty() and not reset:
                continue

            uploading_multiple_entries = package.len > 1
            creating_a_single_empty_dir = (
                package.len == 1 and not package.items[0].is_stream() and os.path.isdir(package.items[0].source)
            )

            if uploading_multiple_entries or creating_a_single_empty_dir or package.is_empty():
                data = compress_to_tar_gz_in_memory(upload_entries=package.items)
                url = build_operation_url(
                    swagger_client.swagger_spec.api_url,
                    swagger_client.api.uploadFileSetAttributeTar.operation.path_name,
                )
                result = upload_raw_data(
                    http_client=swagger_client.swagger_spec.http_client,
                    url=url,
                    data=data,
                    headers={"Content-Type": "application/octet-stream"},
                    query_params={
                        "experimentId": container_id,
                        "attribute": attribute,
                        "reset": str(reset),
                    },
                )
                _attribute_upload_response_handler(result)
            else:
                upload_entry = package.items[0]
                _multichunk_upload_with_retry(
                    upload_entry,
                    query_params={
                        "experimentIdentifier": container_id,
                        "attribute": attribute,
                        "subPath": upload_entry.target_path,
                    },
                    swagger_client=swagger_client,
                    multipart_config=multipart_config,
                    target=FileUploadTarget.FILE_SET,
                )

            reset = False
    except MetadataInconsistency as e:
        if len(e.args) == 1:
            return [e]
        else:
            return [MetadataInconsistency(desc) for desc in e.args]


def get_unique_upload_entries(file_globs: Iterable[str]) -> Set[UploadEntry]:
    absolute_paths = get_absolute_paths(file_globs)
    common_root = get_common_root(absolute_paths)

    upload_entries: List[UploadEntry] = []
    if common_root is not None:
        for absolute_path in absolute_paths:
            upload_entries.append(
                UploadEntry(
                    absolute_path,
                    normalize_file_name(os.path.relpath(absolute_path, common_root)),
                )
            )
    else:
        for absolute_path in absolute_paths:
            upload_entries.append(UploadEntry(absolute_path, normalize_file_name(absolute_path)))

    return scan_unique_upload_entries(upload_entries)


def _attribute_upload_response_handler(result: bytes) -> None:
    try:
        parsed = json.loads(result)
    except json.JSONDecodeError:
        raise InternalClientError("Unexpected response from server: {}".format(result))

    if isinstance(parsed, type(None)):
        # old format with empty optional error
        return
    if isinstance(parsed, dict):
        if "errorDescription" in parsed:
            # old format with optional error
            raise MetadataInconsistency(parsed["errorDescription"])
        elif "errors" in parsed:
            # new format with a list of errors
            error_list = parsed["errors"]
            if isinstance(error_list, list):
                if len(error_list) == 0:
                    return
                try:
                    raise MetadataInconsistency(*[item["errorDescription"] for item in parsed["errors"]])
                except KeyError:
                    # fall into default InternalClientError
                    pass

    raise InternalClientError("Unexpected response from server: {}".format(result))


MultipartUrlSet = collections.namedtuple("MultipartUrlSet", ["start_chunked", "finish_chunked", "send_chunk", "single"])

MULTIPART_URLS = {
    FileUploadTarget.FILE_ATOM: MultipartUrlSet(
        "fileAtomMultipartUploadStart",
        "fileAtomMultipartUploadFinish",
        "fileAtomMultipartUploadPart",
        "fileAtomUpload",
    ),
    FileUploadTarget.FILE_SET: MultipartUrlSet(
        "fileSetFileMultipartUploadStart",
        "fileSetFileMultipartUploadFinish",
        "fileSetFileMultipartUploadPart",
        "fileSetFileUpload",
    ),
}


def _build_multipart_urlset(swagger_client: SwaggerClientWrapper, target: FileUploadTarget) -> MultipartUrlSet:
    urlnameset = MULTIPART_URLS[target]
    return MultipartUrlSet(
        start_chunked=with_api_exceptions_handler(getattr(swagger_client.api, urlnameset.start_chunked)),
        finish_chunked=with_api_exceptions_handler(getattr(swagger_client.api, urlnameset.finish_chunked)),
        send_chunk=build_operation_url(
            swagger_client.swagger_spec.api_url,
            getattr(swagger_client.api, urlnameset.send_chunk).operation.path_name,
        ),
        single=build_operation_url(
            swagger_client.swagger_spec.api_url,
            getattr(swagger_client.api, urlnameset.single).operation.path_name,
        ),
    )


def _multichunk_upload_with_retry(
    upload_entry: UploadEntry,
    swagger_client: SwaggerClientWrapper,
    query_params: dict,
    multipart_config: Optional[MultipartConfig],
    target: FileUploadTarget,
):
    urlset = _build_multipart_urlset(swagger_client, target)
    while True:
        try:
            return _multichunk_upload(upload_entry, swagger_client, query_params, multipart_config, urlset)
        except UploadedFileChanged as e:
            logger.error(str(e))


def _multichunk_upload(
    upload_entry: UploadEntry,
    swagger_client: SwaggerClientWrapper,
    query_params: dict,
    multipart_config: Optional[MultipartConfig],
    urlset: MultipartUrlSet,
):
    if multipart_config is None:
        multipart_config = MultipartConfig.get_default()

    file_stream = upload_entry.get_stream()
    entry_length = upload_entry.length()
    try:
        if entry_length <= multipart_config.max_single_part_size:
            # single upload
            data = file_stream.read()
            result = upload_raw_data(
                http_client=swagger_client.swagger_spec.http_client,
                url=urlset.single,
                data=data,
                query_params=query_params,
            )
            _attribute_upload_response_handler(result)
        else:
            # chunked upload
            result = urlset.start_chunked(**query_params, totalLength=entry_length).response().result
            if result.errors:
                raise MetadataInconsistency([err.errorDescription for err in result.errors])

            no_ext_query_params = query_params.copy()
            if "ext" in no_ext_query_params:
                del no_ext_query_params["ext"]

            upload_id = result.uploadId
            chunker = FileChunker(
                None if upload_entry.is_stream() else upload_entry.source,
                file_stream,
                entry_length,
                multipart_config,
            )
            for idx, chunk in enumerate(chunker.generate()):
                result = upload_raw_data(
                    http_client=swagger_client.swagger_spec.http_client,
                    url=urlset.send_chunk,
                    data=chunk.data,
                    headers={"X-Range": _build_x_range(chunk, entry_length)},
                    query_params={
                        "uploadId": upload_id,
                        "uploadPartIdx": idx,
                        **no_ext_query_params,
                    },
                )
                _attribute_upload_response_handler(result)

            result = urlset.finish_chunked(**no_ext_query_params, uploadId=upload_id).response().result
            if result.errors:
                raise MetadataInconsistency([err.errorDescription for err in result.errors])
        return []
    finally:
        file_stream.close()


def _build_x_range(chunk: FileChunk, total_size: int) -> str:
    return "bytes=%d-%d/%d" % (
        chunk.start,
        chunk.end - 1,
        total_size,
    )


@with_api_exceptions_handler
def upload_raw_data(
    http_client: RequestsClient,
    url: str,
    data: AnyStr,
    path_params: Optional[Dict[str, str]] = None,
    query_params: Optional[Dict[str, str]] = None,
    headers: Optional[Dict[str, str]] = None,
):
    url = _generate_url(url=url, path_params=path_params, query_params=query_params)

    session = http_client.session
    request = http_client.authenticator.apply(Request(method="POST", url=url, data=data, headers=headers))
    response = handle_server_raw_response_messages(session.send(session.prepare_request(request)))

    if response.status_code >= 300:
        ApiMethodWrapper.handle_neptune_http_errors(response)
    if response.status_code in (
        HTTPUnprocessableEntity.status_code,
        HTTPPaymentRequired.status_code,
    ):
        raise NeptuneLimitExceedException(reason=response.json().get("title", "Unknown reason"))
    response.raise_for_status()

    return response.content


def download_image_series_element(
    swagger_client: SwaggerClientWrapper,
    container_id: str,
    attribute: str,
    index: int,
    destination: str,
    progress_bar: Optional[ProgressBarType],
):
    url = build_operation_url(
        swagger_client.swagger_spec.api_url,
        swagger_client.api.getImageSeriesValue.operation.path_name,
    )
    response = _download_raw_data(
        http_client=swagger_client.swagger_spec.http_client,
        url=url,
        headers={},
        query_params={
            "experimentId": container_id,
            "attribute": attribute,
            "index": index,
        },
    )
    _store_response_as_file(
        response,
        os.path.join(
            destination,
            "{}.{}".format(index, response.headers["content-type"].split("/")[-1]),
        ),
        progress_bar=progress_bar,
    )


def download_file_attribute(
    swagger_client: SwaggerClientWrapper,
    container_id: str,
    attribute: str,
    destination: Optional[str] = None,
    progress_bar: Optional[ProgressBarType] = None,
):
    url = build_operation_url(
        swagger_client.swagger_spec.api_url,
        swagger_client.api.downloadAttribute.operation.path_name,
    )
    response = _download_raw_data(
        http_client=swagger_client.swagger_spec.http_client,
        url=url,
        headers={"Accept": "application/octet-stream"},
        query_params={"experimentId": container_id, "attribute": attribute},
    )
    _store_response_as_file(response, destination, progress_bar)


def download_file_set_attribute(
    swagger_client: SwaggerClientWrapper,
    download_id: str,
    destination: Optional[str] = None,
    progress_bar: Optional[ProgressBarType] = None,
):
    download_url: Optional[str] = _get_download_url(swagger_client, download_id)
    next_sleep = 0.5
    while download_url is None:
        time.sleep(next_sleep)
        next_sleep = min(2 * next_sleep, 5)
        download_url = _get_download_url(swagger_client, download_id)

    response = _download_raw_data(
        http_client=swagger_client.swagger_spec.http_client,
        url=download_url,
        headers={"Accept": "application/zip"},
    )
    _store_response_as_file(response, destination, progress_bar)


def _get_download_url(swagger_client: SwaggerClientWrapper, download_id: str):
    params = {"id": download_id}
    download_request = swagger_client.api.getDownloadPrepareRequest(**params).response().result
    return download_request.downloadUrl


def _store_response_as_file(
    response: Response,
    destination: Optional[str] = None,
    progress_bar: Optional[ProgressBarType] = None,
) -> None:
    chunk_size = 1024 * 1024

    if destination is None:
        target_file = _get_content_disposition_filename(response)
    elif os.path.isdir(destination):
        target_file = os.path.join(destination, _get_content_disposition_filename(response))
    else:
        target_file = destination

    if "content-length" in response.headers:
        total_size = int(response.headers["content-length"])
        progress_bar = False if total_size < chunk_size else progress_bar  # less than one chunk
    else:
        total_size = 0

    # TODO: update syntax once py3.10 becomes min supported version (with (x(), y(), z()): ...)
    with ExitStack() as stack:
        bar = stack.enter_context(construct_progress_bar(progress_bar, "Fetching file..."))
        response = stack.enter_context(response)
        file_stream = stack.enter_context(open(target_file, "wb"))

        for chunk in response.iter_content(chunk_size=chunk_size):
            if chunk:
                file_stream.write(chunk)
                bar.update(by=len(chunk), total=total_size)


def _get_content_disposition_filename(response: Response) -> str:
    content_disposition = response.headers["Content-Disposition"]
    return content_disposition[content_disposition.rfind("filename=") + 9 :].strip('"')


@with_api_exceptions_handler
def _download_raw_data(
    http_client: RequestsClient,
    url: str,
    path_params: Optional[Dict[str, str]] = None,
    query_params: Optional[Dict[str, str]] = None,
    headers: Optional[Dict[str, str]] = None,
) -> Response:
    url = _generate_url(url=url, path_params=path_params, query_params=query_params)

    session = http_client.session
    request = http_client.authenticator.apply(Request(method="GET", url=url, headers=headers))

    response = handle_server_raw_response_messages(session.send(session.prepare_request(request), stream=True))

    response.raise_for_status()
    return response


def _generate_url(
    url: str,
    path_params: Optional[Dict[str, str]] = None,
    query_params: Optional[Dict[str, str]] = None,
) -> str:
    for key, val in (path_params or dict()).items():
        url = url.replace("{" + key + "}", val)
    if query_params:
        url = url + "?" + urlencode(list(query_params.items()))
    return url
