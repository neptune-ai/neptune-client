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
__all__ = ["custom_run_id_exceeds_length", "image_size_exceeds_limit_for_logging"]

import warnings

from neptune.internal.utils.logger import get_logger

_logger = get_logger()


_CUSTOM_RUN_ID_LENGTH = 36
_LOGGED_IMAGE_SIZE_LIMIT_MB = 32

BYTES_IN_MB = 1024 * 1024


def custom_run_id_exceeds_length(custom_run_id):
    if custom_run_id and len(custom_run_id) > _CUSTOM_RUN_ID_LENGTH:
        _logger.warning(
            "Given custom_run_id exceeds %s" " characters and it will be ignored.",
            _CUSTOM_RUN_ID_LENGTH,
        )
        return True
    return False


def image_size_exceeds_limit_for_logging(content_size):
    if content_size > _LOGGED_IMAGE_SIZE_LIMIT_MB * BYTES_IN_MB:
        warnings.warn(
            f"You are attempting to log an image that is {content_size / BYTES_IN_MB:.2f}MB large. "
            f"Neptune supports logging images smaller than {_LOGGED_IMAGE_SIZE_LIMIT_MB}MB. "
            "Resize or increase compression of this image.",
            category=UserWarning,
        )
        return True
    return False
