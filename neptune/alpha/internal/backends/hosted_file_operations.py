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
import uuid
from typing import List, Optional, Dict

from bravado.requests_client import RequestsClient
from requests import Request, Response
from bravado.client import SwaggerClient

from neptune.alpha.exceptions import FileUploadError
from neptune.alpha.internal.backends.utils import with_api_exceptions_handler
from neptune.internal.storage.datastream import compress_to_tar_gz_in_memory, FileChunkStream, FileChunk
from neptune.internal.storage.storage_utils import scan_unique_upload_entries, split_upload_files, UploadEntry


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


# NOT USED RIGHT NOW: USE IN FILE SETS OR DELETE
def upload_file_attributes(experiment_uuid: uuid.UUID,
                           upload_entries: List[UploadEntry],
                           swagger_client: SwaggerClient
                           ) -> List[FileUploadError]:
    result = []
    existing_entries = []

    # NOTICE: After following check there still is a small window in which file can be deleted.
    for entry in upload_entries:
        if not os.path.isfile(entry.source_path):
            result.append(FileUploadError(entry.source_path, "Path not found or is a not a file."))
        else:
            existing_entries.append(entry)

    if not existing_entries:
        return result

    try:
        unique_upload_entries = scan_unique_upload_entries(existing_entries)

        for package in split_upload_files(unique_upload_entries):
            if package.is_empty():
                continue

            uploading_multiple_entries = package.len > 1
            creating_a_single_empty_dir = package.len == 1 and not package.items[0].is_stream() \
                                          and os.path.isdir(package.items[0].source_path)

            if uploading_multiple_entries or creating_a_single_empty_dir:
                data = compress_to_tar_gz_in_memory(upload_entries=package.items)
                url = swagger_client.swagger_spec.api_url + swagger_client.api.uploadTarStream.operation.path_name
                upload_raw_data(http_client=swagger_client.swagger_spec.http_client,
                                url=url,
                                data=io.BytesIO(data),
                                query_params={
                                    "experimentIdentifier": str(experiment_uuid),
                                    "resource": "attributes",
                                })
            else:
                url = swagger_client.swagger_spec.api_url + swagger_client.api.uploadPath.operation.path_name
                file_chunk_stream = FileChunkStream(package.items[0])
                _upload_loop(http_client=swagger_client.swagger_spec.http_client,
                             url=url,
                             file_chunk_stream=file_chunk_stream,
                             query_params={
                                 "experimentIdentifier": str(experiment_uuid),
                                 "resource": "attributes",
                                 "pathParam": file_chunk_stream.filename
                             })

            return result
    except Exception as e:
        msg = getattr(e, 'message', repr(e))
        return [FileUploadError(entry.source_path, msg) for entry in upload_entries]


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
    for key, val in (path_params or dict()).items():
        url = url.replace("{" + key + "}", val)
    if query_params:
        url = url + "?"
        for key, val in query_params.items():
            url = url + key + "=" + val + "&"

    session = http_client.session
    request = http_client.authenticator.apply(Request(method='POST', url=url, data=data, headers=headers))

    response = session.send(session.prepare_request(request))
    response.raise_for_status()


@with_api_exceptions_handler
def download_file_attribute(swagger_client: SwaggerClient,
                            experiment_uuid: uuid.UUID,
                            attribute: str,
                            file_path: Optional[str] = None):
    response = _download_raw_data(
        http_client=swagger_client.swagger_spec.http_client,
        url=swagger_client.swagger_spec.api_url + swagger_client.api.downloadAttribute.operation.path_name,
        headers={"Accept": "application/octet-stream"},
        query_params={"experimentId": str(experiment_uuid), "attribute": attribute})
    with response:
        with open(file_path or _get_content_disposition_filename(response), "wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)


def _get_content_disposition_filename(response: Response) -> str:
    content_disposition = response.headers['Content-Disposition']
    return content_disposition[content_disposition.rfind("filename=")+9:]


def _download_raw_data(http_client: RequestsClient,
                       url: str,
                       path_params: Optional[Dict[str, str]] = None,
                       query_params: Optional[Dict[str, str]] = None,
                       headers: Optional[Dict[str, str]] = None
                       ) -> Response:
    for key, val in (path_params or dict()).items():
        url = url.replace("{" + key + "}", val)
    if query_params:
        url = url + "?"
        for key, val in query_params.items():
            url = url + key + "=" + val + "&"

    session = http_client.session
    request = http_client.authenticator.apply(Request(method='GET', url=url, headers=headers))

    response = session.send(session.prepare_request(request), stream=True)
    response.raise_for_status()
    return response
