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
__all__ = ["in_interactive", "in_notebook"]

import sys


def in_interactive() -> bool:
    """Based on: https://stackoverflow.com/a/2356427/1565454"""
    return hasattr(sys, "ps1")


def in_notebook() -> bool:
    """Based on: https://stackoverflow.com/a/22424821/1565454"""
    try:
        from IPython import get_ipython

        ipy = get_ipython()
        return (
            ipy is not None and hasattr(ipy, "config") and isinstance(ipy.config, dict) and "IPKernelApp" in ipy.config
        )
    except ImportError:
        return False
