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
from typing import List

import requests
from bravado.client import SwaggerClient

from neptune.alpha.exceptions import FileUploadError
from neptune.alpha.internal.backends.utils import with_api_exceptions_handler
from neptune.internal.storage.datastream import compress_to_tar_gz_in_memory, FileChunkStream
from neptune.internal.storage.storage_utils import scan_unique_upload_entries, \
    split_upload_files, UploadEntry


def upload_file_attributes(experiment_uuid: uuid.UUID,
                           upload_entries: List[UploadEntry],
                           swagger_client: SwaggerClient) -> List[FileUploadError]:
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
                                headers=dict(),
                                query_params={
                                    "experimentIdentifier": str(experiment_uuid),
                                    "resource": "attributes",
                                })
            else:
                file_chunk_stream = FileChunkStream(package.items[0])
                url = swagger_client.swagger_spec.api_url + swagger_client.api.uploadPath.operation.path_name
                _upload_loop(http_client=swagger_client.swagger_spec.http_client,
                             url=url,
                             data=file_chunk_stream,
                             query_params={
                                 "experimentIdentifier": str(experiment_uuid),
                                 "resource": "attributes",
                                 "pathParam": file_chunk_stream.filename
                             })

            return result
    except Exception as e:
        msg = getattr(e, 'message', repr(e))
        return [FileUploadError(entry.source_path, msg) for entry in upload_entries]


def _upload_loop(data, **kwargs):
    ret = None
    for part in data.generate():
        part_to_send = part.get_data()
        ret = _upload_loop_chunk(part, part_to_send, data, **kwargs)

    data.close()
    return ret


def _upload_loop_chunk(part, part_to_send, data, **kwargs):
    if data.length is not None:
        binary_range = "bytes=%d-%d/%d" % (part.start, part.end - 1, data.length)
    else:
        binary_range = "bytes=%d-%d" % (part.start, part.end - 1)
    headers = {
        "Content-Filename": data.filename,
        "X-Range": binary_range,
    }
    if data.permissions is not None:
        headers["X-File-Permissions"] = data.permissions
    return upload_raw_data(data=part_to_send, headers=headers, **kwargs)


@with_api_exceptions_handler
def upload_raw_data(http_client, url, data, headers, path_params=None, query_params=None):
    url = url + "?"

    for key, val in (path_params or dict()).items():
        url = url.replace("{" + key + "}", val)

    for key, val in (query_params or dict()).items():
        url = url + key + "=" + val + "&"

    headers["Content-Type"] = "application/octet-stream"

    session = http_client.session

    request = http_client.authenticator.apply(
        requests.Request(
            method='POST',
            url=url,
            data=data,
            headers=headers
        )
    )

    response = session.send(session.prepare_request(request))
    response.raise_for_status()
    return response
