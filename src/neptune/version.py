#
# Copyright (c) 2023, Neptune Labs Sp. z o.o.
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
__all__ = ["version", "__version__"]

import sys
from typing import Optional

from packaging.version import parse

if sys.version_info >= (3, 8):
    from importlib.metadata import PackageNotFoundError
    from importlib.metadata import version as version_parser
else:
    from importlib_metadata import PackageNotFoundError
    from importlib_metadata import version as version_parser


def check_version(package_name: str) -> Optional[str]:
    try:
        return version_parser(package_name)
    except PackageNotFoundError:
        # package is not installed
        return None


def detect_version() -> str:
    neptune_version = check_version("neptune")

    if neptune_version is not None:
        return neptune_version

    raise PackageNotFoundError("neptune")


__version__ = detect_version()
version = parse(__version__)
