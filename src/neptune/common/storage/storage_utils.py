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
import io
import logging
import os
import stat
import time
from abc import (
    ABCMeta,
    abstractmethod,
)
from dataclasses import dataclass
from io import BytesIO
from pprint import pformat
from typing import (
    BinaryIO,
    Generator,
    List,
    Set,
    Union,
)

import six

_logger = logging.getLogger(__name__)


@dataclass
class AttributeUploadConfiguration:
    chunk_size: int


class UploadEntry(object):
    def __init__(self, source: Union[str, BytesIO], target_path: str):
        self.source = source
        self.target_path = target_path

    def length(self) -> int:
        if self.is_stream():
            return self.source.getbuffer().nbytes
        else:
            return os.path.getsize(self.source)

    def get_stream(self) -> Union[BinaryIO, io.BytesIO]:
        if self.is_stream():
            return self.source
        else:
            return io.open(self.source, "rb")

    def get_permissions(self) -> str:
        if self.is_stream():
            return "----------"
        else:
            return self.permissions_to_unix_string(self.source)

    @classmethod
    def permissions_to_unix_string(cls, path):
        st = 0
        if os.path.exists(path):
            st = os.lstat(path).st_mode
        is_dir = "d" if stat.S_ISDIR(st) else "-"
        dic = {
            "7": "rwx",
            "6": "rw-",
            "5": "r-x",
            "4": "r--",
            "3": "-wx",
            "2": "-w-",
            "1": "--x",
            "0": "---",
        }
        perm = ("%03o" % st)[-3:]
        return is_dir + "".join(dic.get(x, x) for x in perm)

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
        return hash((self.source, self.target_path))

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

    def is_stream(self):
        return hasattr(self.source, "read")


class UploadPackage(object):
    def __init__(self):
        self.items: List[UploadEntry] = []
        self.size: int = 0
        self.len: int = 0

    def reset(self):
        self.items = []
        self.size = 0
        self.len = 0

    def update(self, entry: UploadEntry, size: int):
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


@six.add_metaclass(ABCMeta)
class ProgressIndicator(object):
    @abstractmethod
    def progress(self, steps):
        pass

    @abstractmethod
    def complete(self):
        pass


class LoggingProgressIndicator(ProgressIndicator):
    def __init__(self, total, frequency=10):
        self.current = 0
        self.total = total
        self.last_warning = time.time()
        self.frequency = frequency
        _logger.warning(
            "You are sending %dMB of source code to Neptune. "
            "It is pretty uncommon - please make sure it's what you wanted.",
            self.total / (1024 * 1024),
        )

    def progress(self, steps):
        self.current += steps
        if time.time() - self.last_warning > self.frequency:
            _logger.warning(
                "%d MB / %d MB (%d%%) of source code was sent to Neptune.",
                self.current / (1024 * 1024),
                self.total / (1024 * 1024),
                100 * self.current / self.total,
            )
            self.last_warning = time.time()

    def complete(self):
        _logger.warning(
            "%d MB (100%%) of source code was sent to Neptune.",
            self.total / (1024 * 1024),
        )


class SilentProgressIndicator(ProgressIndicator):
    def __init__(self):
        pass

    def progress(self, steps):
        pass

    def complete(self):
        pass


def scan_unique_upload_entries(upload_entries):
    """
    Returns upload entries for all files that could be found for given upload entries.
    In case of directory as upload entry, files we be taken from all subdirectories recursively.
    Any duplicated entries are removed.
    """
    walked_entries = set()
    for entry in upload_entries:
        if entry.is_stream() or not os.path.isdir(entry.source):
            walked_entries.add(entry)
        else:
            for root, _, files in os.walk(entry.source):
                path_relative_to_entry_source = os.path.relpath(root, entry.source)
                target_root = os.path.normpath(os.path.join(entry.target_path, path_relative_to_entry_source))
                for filename in files:
                    walked_entries.add(
                        UploadEntry(
                            os.path.join(root, filename),
                            os.path.join(target_root, filename),
                        )
                    )

    return walked_entries


def split_upload_files(
    upload_entries: Set[UploadEntry],
    upload_configuration: AttributeUploadConfiguration,
    max_files=500,
) -> Generator[UploadPackage, None, None]:
    current_package = UploadPackage()

    for entry in upload_entries:
        if entry.is_stream():
            if current_package.len > 0:
                yield current_package
                current_package.reset()
            current_package.update(entry, 0)
            yield current_package
            current_package.reset()
        else:
            size = os.path.getsize(entry.source)
            if (
                size + current_package.size > upload_configuration.chunk_size or current_package.len > max_files
            ) and not current_package.is_empty():
                yield current_package
                current_package.reset()
            current_package.update(entry, size)

    yield current_package


def normalize_file_name(name):
    return name.replace(os.sep, "/")
