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

from neptune.common.warnings import warn_once

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
    neptune_client_version = check_version("neptune-client")

    if neptune_version is not None and neptune_client_version is not None:
        raise RuntimeError(
            "We've detected that the 'neptune' and 'neptune-client' packages are both installed. "
            "Uninstall each of them and then install only the new 'neptune' package. For more information, "
            "see https://docs.neptune.ai/setup/upgrading/"
        )
    elif neptune_version is not None:
        return neptune_version
    elif neptune_client_version is not None:
        warn_once(
            "The 'neptune-client' package has been deprecated and will be removed in the future. Install "
            "the 'neptune' package instead. For more, see https://docs.neptune.ai/setup/upgrading/"
        )
        return neptune_client_version

    raise PackageNotFoundError("neptune")


__version__ = detect_version()
version = parse(__version__)
