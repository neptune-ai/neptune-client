#
# Copyright (c) 2021, Neptune Labs Sp. z o.o.
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
import hashlib
import pathlib

CHUNK_SIZE = 4096


# from https://stackoverflow.com/a/3431838
def md5(fname):
    hash_md5 = hashlib.md5()

    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(CHUNK_SIZE), b""):
            hash_md5.update(chunk)

    return hash_md5.hexdigest()


def append_non_relative_path(
    base_path: pathlib.Path, path_to_append: str
) -> pathlib.Path:
    # By default when second path starts with '/', it replaces the path we're appending to
    relative_path = (
        path_to_append[1:] if path_to_append.startswith("/") else path_to_append
    )
    return base_path / relative_path
