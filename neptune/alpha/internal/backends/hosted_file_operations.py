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
import io
import os
import time
import uuid
from glob import glob
from typing import List, Optional, Dict, Iterable, Set
from urllib.parse import urlencode

from bravado.client import SwaggerClient
from bravado.requests_client import RequestsClient
from requests import Request, Response

from neptune.alpha.exceptions import FileUploadError
from neptune.alpha.internal.backends.utils import with_api_exceptions_handler
from neptune.internal.storage.datastream import compress_to_tar_gz_in_memory, FileChunkStream, FileChunk
from neptune.internal.storage.storage_utils import scan_unique_upload_entries, split_upload_files, UploadEntry, \
    normalize_file_name


def upload_file_attribute(swagger_client: SwaggerClient,
                          experiment_uuid: uuid.UUID,
                          attribute: str,
                          file_path: str
                          ) -> Optional[FileUploadError]:
    if not os.path.isfile(file_path):
        return FileUploadError(file_path, "Path not found or is a not a file.")
    try:
        url = swagger_client.swagger_spec.api_url + swagger_client.api.uploadAttribute.operation.path_name
        _upload_loop(http_client=swagger_client.swagger_spec.http_client,
                     url=url,
                     file_chunk_stream=FileChunkStream(UploadEntry(file_path, file_path)),
                     query_params={
                         "experimentId": str(experiment_uuid),
                         "attribute": attribute,
                         "filename": os.path.basename(file_path)
                     })
    except Exception as e:
        return FileUploadError(file_path, getattr(e, 'message', repr(e)))


def upload_file_set_attribute(swagger_client: SwaggerClient,
                              experiment_uuid: uuid.UUID,
                              attribute: str,
                              file_globs: Iterable[str],
                              reset: bool,
                              ) -> List[FileUploadError]:
    unique_upload_entries = get_unique_upload_entries(file_globs)
    result: List[FileUploadError] = []

    try:
        for package in split_upload_files(unique_upload_entries):
            if package.is_empty() and not reset:
                continue

            uploading_multiple_entries = package.len > 1
            creating_a_single_empty_dir = package.len == 1 and not package.items[0].is_stream() \
                                          and os.path.isdir(package.items[0].source_path)

            if uploading_multiple_entries or creating_a_single_empty_dir or package.is_empty():
                data = compress_to_tar_gz_in_memory(upload_entries=package.items)
                url = swagger_client.swagger_spec.api_url \
                      + swagger_client.api.uploadFileSetAttributeTar.operation.path_name
                upload_raw_data(http_client=swagger_client.swagger_spec.http_client,
                                url=url,
                                data=io.BytesIO(data),
                                headers={"Content-Type": "application/octet-stream"},
                                query_params={
                                    "experimentId": str(experiment_uuid),
                                    "attribute": attribute,
                                    "reset": str(reset)
                                })
            else:
                url = swagger_client.swagger_spec.api_url \
                      + swagger_client.api.uploadFileSetAttributeChunk.operation.path_name
                file_chunk_stream = FileChunkStream(package.items[0])
                _upload_loop(http_client=swagger_client.swagger_spec.http_client,
                             url=url,
                             file_chunk_stream=file_chunk_stream,
                             query_params={
                                 "experimentId": str(experiment_uuid),
                                 "attribute": attribute,
                                 "reset": str(reset),
                                 "path": file_chunk_stream.filename
                             })

            reset = False

        return result
    except Exception as e:
        msg = getattr(e, 'message', repr(e))
        return [FileUploadError(entry.source_path, msg) for entry in unique_upload_entries]


def get_unique_upload_entries(file_globs: Iterable[str]) -> Set[UploadEntry]:
    expanded_paths: Set[str] = set()
    for file_glob in file_globs:
        expanded_paths |= set(glob(file_glob))
    absolute_paths = list(os.path.abspath(expanded_file) for expanded_file in expanded_paths)

    upload_entries: List[UploadEntry] = []
    try:
        common_root = os.path.commonpath(absolute_paths)
    except ValueError:
        for absolute_path in absolute_paths:
            upload_entries.append(UploadEntry(absolute_path, normalize_file_name(absolute_path)))
    else:
        if os.path.isfile(common_root):
            common_root = os.path.dirname(common_root)
        if common_root.startswith(os.getcwd() + os.sep):
            common_root = os.getcwd()
        for absolute_path in absolute_paths:
            upload_entries.append(UploadEntry(absolute_path, normalize_file_name(
                os.path.relpath(absolute_path, common_root))))

    return scan_unique_upload_entries(upload_entries)


def _upload_loop(file_chunk_stream: FileChunkStream, **kwargs):
    for chunk in file_chunk_stream.generate():
        _upload_loop_chunk(chunk, file_chunk_stream, **kwargs)

    file_chunk_stream.close()


def _upload_loop_chunk(chunk: FileChunk, file_chunk_stream: FileChunkStream, **kwargs):
    if file_chunk_stream.length is not None:
        binary_range = "bytes=%d-%d/%d" % (chunk.start, chunk.end - 1, file_chunk_stream.length)
    else:
        binary_range = "bytes=%d-%d" % (chunk.start, chunk.end - 1)
    headers = {
        "Content-Filename": file_chunk_stream.filename,
        "X-Range": binary_range,
        "Content-Type": "application/octet-stream"
    }
    if file_chunk_stream.permissions is not None:
        headers["X-File-Permissions"] = file_chunk_stream.permissions
    upload_raw_data(data=chunk.get_data(), headers=headers, **kwargs)


@with_api_exceptions_handler
def upload_raw_data(http_client: RequestsClient,
                    url: str,
                    data,
                    path_params: Optional[Dict[str, str]] = None,
                    query_params: Optional[Dict[str, str]] = None,
                    headers: Optional[Dict[str, str]] = None):
    url = _generate_url(url=url, path_params=path_params, query_params=query_params)

    session = http_client.session
    request = http_client.authenticator.apply(Request(method='POST', url=url, data=data, headers=headers))

    response = session.send(session.prepare_request(request))
    response.raise_for_status()


@with_api_exceptions_handler
def download_file_attribute(swagger_client: SwaggerClient,
                            experiment_uuid: uuid.UUID,
                            attribute: str,
                            destination: Optional[str] = None):
    response = _download_raw_data(
        http_client=swagger_client.swagger_spec.http_client,
        url=swagger_client.swagger_spec.api_url + swagger_client.api.downloadAttribute.operation.path_name,
        headers={"Accept": "application/octet-stream"},
        query_params={"experimentId": str(experiment_uuid), "attribute": attribute})
    _store_response_as_file(response, destination)


@with_api_exceptions_handler
def download_zip(swagger_client: SwaggerClient,
                 download_id: uuid.UUID,
                 destination: Optional[str] = None):
    download_url: Optional[str] = _get_download_url(swagger_client, download_id)
    next_sleep = 0.5
    while download_url is None:
        time.sleep(next_sleep)
        next_sleep = min(2 * next_sleep, 5)
        download_url = _get_download_url(swagger_client, download_id)

    response = _download_raw_data(
        http_client=swagger_client.swagger_spec.http_client,
        url=download_url,
        headers={"Accept": "application/zip"})
    _store_response_as_file(response, destination)


def _get_download_url(swagger_client: SwaggerClient, download_id: uuid.UUID):
    params = {"id": str(download_id)}
    download_request = swagger_client.api.getDownloadPrepareRequest(**params).response().result
    return download_request.downloadUrl


def _store_response_as_file(response: Response, destination: Optional[str] = None):
    if destination is None:
        target_file = _get_content_disposition_filename(response)
    elif os.path.isdir(destination):
        target_file = os.path.join(destination, _get_content_disposition_filename(response))
    else:
        target_file = destination
    with response:
        with open(target_file, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)


def _get_content_disposition_filename(response: Response) -> str:
    content_disposition = response.headers['Content-Disposition']
    return content_disposition[content_disposition.rfind("filename=") + 9:]


def _download_raw_data(http_client: RequestsClient,
                       url: str,
                       path_params: Optional[Dict[str, str]] = None,
                       query_params: Optional[Dict[str, str]] = None,
                       headers: Optional[Dict[str, str]] = None
                       ) -> Response:
    url = _generate_url(url=url, path_params=path_params, query_params=query_params)

    session = http_client.session
    request = http_client.authenticator.apply(Request(method='GET', url=url, headers=headers))

    response = session.send(session.prepare_request(request), stream=True)
    response.raise_for_status()
    return response


def _generate_url(url: str,
                  path_params: Optional[Dict[str, str]] = None,
                  query_params: Optional[Dict[str, str]] = None,
                  ) -> str:
    for key, val in (path_params or dict()).items():
        url = url.replace("{" + key + "}", val)
    if query_params:
        url = url + "?" + urlencode(list(query_params.items()))
    return url
