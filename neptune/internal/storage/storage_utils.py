#
# Copyright (c) 2019, Neptune Labs Sp. z o.o.
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
from pprint import pformat

from neptune.internal.storage.datastream import compress_to_tar_gz_in_memory, FileChunkStream


class UploadEntry(object):
    def __init__(self, source_path, target_path):
        self.source_path = source_path
        self.target_path = target_path

    def __eq__(self, other):
        """
        Returns true if both objects are equal
        """
        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        """
        Returns true if both objects are not equal
        """
        return not self == other

    def __hash__(self):
        """
        Returns the hash of source and target path
        """
        return hash((self.source_path, self.target_path))

    def to_str(self):
        """
        Returns the string representation of the model
        """
        return pformat(self.__dict__)

    def __repr__(self):
        """
        For `print` and `pprint`
        """
        return self.to_str()


class UploadPackage(object):
    def __init__(self):
        self.items = []
        self.size = 0
        self.len = 0

    def reset(self):
        self.items = []
        self.size = 0
        self.len = 0

    def update(self, entry, size):
        self.items.append(entry)
        self.size += size
        self.len += 1

    def is_empty(self):
        return self.len == 0

    def __eq__(self, other):
        """
        Returns true if both objects are equal
        """
        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        """
        Returns true if both objects are not equal
        """
        return not self == other

    def to_str(self):
        """
        Returns the string representation of the model
        """
        return pformat(self.__dict__)

    def __repr__(self):
        """
        For `print` and `pprint`
        """
        return self.to_str()


def scan_unique_upload_entries(upload_entries):
    """
    Returns upload entries for all files that could be found for given upload entries.
    In case of directory as upload entry, files we be taken from all subdirectories recursively.
    Any duplicated entries are removed.
    """
    walked_entries = set()
    for entry in upload_entries:
        if os.path.isdir(entry.source_path):
            for root, _, files in os.walk(entry.source_path):
                path_relative_to_entry_source = os.path.relpath(root, entry.source_path)
                target_root = os.path.normpath(os.path.join(entry.target_path, path_relative_to_entry_source))
                for filename in files:
                    walked_entries.add(UploadEntry(os.path.join(root, filename), os.path.join(target_root, filename)))
        else:
            walked_entries.add(entry)
    return walked_entries


def split_upload_files(upload_entries, max_package_size=1 * 1024 * 1024, max_files=500):
    current_package = UploadPackage()

    for entry in upload_entries:
        size = os.path.getsize(entry.source_path)

        if (size + current_package.size > max_package_size or current_package.len > max_files) \
                and not current_package.is_empty():
            yield current_package
            current_package.reset()
        current_package.update(entry, size)

    yield current_package


def normalize_file_name(name):
    return name.replace(os.sep, '/')


def upload_to_storage(upload_entries, upload_api_fun, upload_tar_api_fun, **kwargs):
    for package in split_upload_files(scan_unique_upload_entries(upload_entries)):
        if package.is_empty():
            break

        uploading_multiple_entries = package.len > 1
        creating_a_single_empty_dir = package.len == 1 and os.path.isdir(package.items[0].source_path)

        if uploading_multiple_entries or creating_a_single_empty_dir:
            data = compress_to_tar_gz_in_memory(upload_entries=package.items)
            upload_tar_api_fun(**dict(kwargs, data=data))
        else:
            file_chunk_stream = FileChunkStream(package.items[0])
            upload_api_fun(**dict(kwargs, data=file_chunk_stream))
