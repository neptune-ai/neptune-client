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
import contextlib
from tempfile import NamedTemporaryFile
import os


@contextlib.contextmanager
def create_file(content=None, binary_mode=False) -> str:
    """
    A lot this is motivated by:
    Whether the name can be used to open the file a second time,
    while the named temporary file is still open, varies across platforms
    (it can be so used on Unix; it cannot on Windows NT or later).
     ref. https://docs.python.org/3.9/library/tempfile.html#tempfile.NamedTemporaryFile
    """
    if binary_mode:
        mode = "wb"
    else:
        mode = "w"
    with NamedTemporaryFile(mode, delete=False) as file:
        if content:
            file.write(content)
        file.close()
        yield file.name
        os.remove(file.name)
