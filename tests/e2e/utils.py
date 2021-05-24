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
__all__ = [
    'with_check_if_file_appears',
]

import os
from contextlib import contextmanager


def _remove_file_if_exists(filepath):
    try:
        os.remove(filepath)
    except OSError:
        pass


@contextmanager
def with_check_if_file_appears(filepath):
    """Checks if file will be present when leaving the block.
    File is removed if exists when entering the block."""
    _remove_file_if_exists(filepath)

    yield

    assert os.path.exists(filepath)
    _remove_file_if_exists(filepath)
