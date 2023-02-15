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
__all__ = ["sha1"]

import hashlib
import pathlib
import typing


def sha1(fname: typing.Union[str, pathlib.Path], block_size: int = 2**16) -> str:
    sha1sum = hashlib.sha1()

    with open(fname, "rb") as source:
        block = source.read(block_size)

        while len(block) != 0:
            sha1sum.update(block)
            block = source.read(block_size)

    return str(sha1sum.hexdigest())
